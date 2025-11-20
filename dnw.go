package tensorutils

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/JoshuaDoes/crunchio"
	"go.bug.st/serial"
	"go.bug.st/serial/enumerator"
)

var (
	devicePairsDNW = [][]string{
		{"18D1", "4F00"}, //Google Pixel 6/6a/6Pro
	}
	claimDNW = make(map[string]*DNW)
)

// GetDNW finds the next device known to be compatible with DNW and claims it
func GetDNW() (*DNW, error) {
	closeGhostsDNW()

	//Find a matching device pair available for DNW
	for i := 0; i < len(devicePairsDNW); i++ {
		devPair := devicePairsDNW[i]
		dev := getDevice(devPair[0], devPair[1])
		if dev == nil {
			continue
		}

		//Skip the device if it was already claimed for DNW
		if _, exists := claimDNW[dev.Name]; exists {
			continue
		}

		port, err := serial.Open(dev.Name, &serial.Mode{BaudRate: 115200, Parity: serial.NoParity, DataBits: 8, StopBits: serial.OneStopBit})
		if err != nil {
			return nil, fmt.Errorf("dnw: failed to claim '%s': %v", dev.Name, err)
		}
		port.SetReadTimeout(time.Millisecond * 200)

		//Claim the DNW device
		dnw := new(DNW)
		dnw.port = port
		dnw.info = dev
		claimDNW[dev.Name] = dnw

		//Lock the device mutex until the reader thread is started
		dnw.mutex.Lock()

		//Start the reader thread
		go dnw.readThread()

		return dnw, nil
	}
	return nil, fmt.Errorf("dnw: no device found")
}

func RegisterDevicePairDNW(vid, pid string) {
	found := false
	for _, pair := range devicePairsDNW {
		if pair[0] == vid && pair[1] == pid {
			found = true
			break
		}
	}
	if !found {
		devicePairsDNW = append(devicePairsDNW, []string{vid, pid})
	}
}

func closeGhostsDNW() {
	refreshDevices()
	for port, dnw := range claimDNW {
		found := false
		for i := 0; i < len(knownDevices); i++ {
			if knownDevices[i].Name == port {
				found = true
				break
			}
		}

		if !found {
			dnw.close()
			delete(claimDNW, port)
		}
	}
}

type DNW struct {
	port serial.Port
	info *enumerator.PortDetails

	buffer *crunchio.Buffer //Used for writing the message queue
	reader *crunchio.Buffer //Clone for reading the message queue

	mutex  sync.Mutex
	closed bool
}

func (dnw *DNW) ReadMsg() (*Message, error) {
	dnw.mutex.Lock()
	defer dnw.mutex.Unlock()

	//Read from a clone of the message queue
	return dnw.readMsg(dnw.reader)
}

func (dnw *DNW) readMsg(r *crunchio.Buffer) (*Message, error) {
	if dnw.buffer == nil {
		return nil, fmt.Errorf("dnw: buffer was freed")
	}

	buf := make([]byte, 0)
	for {
		p := make([]byte, 1)
		n, err := dnw.read(r, p)
		if err != nil {
			if len(buf) > 0 {
				return NewMessage(buf), err
			}
			return nil, err
		}
		if n != 1 {
			if len(buf) > 0 {
				//We haven't read a full message yet, but we have some data!
				//Seek backwards and return an empty message so caller can try again on loop
				r.Seek(int64(-1*len(buf)), io.SeekCurrent)
				return nil, nil
			}

			//Nothing was read yet
			if dnw.Closed() {
				break
			}
			continue
		}
		b := p[0]

		if b == '\n' || b == '\r' {
			if len(buf) > 0 {
				break //Parse what we've read so far
			}
			continue //Skip empty queued messages
		}

		buf = append(buf, b)
	}

	if len(buf) > 0 {
		return NewMessage(buf), nil
	}
	return nil, nil
}
func (dnw *DNW) Read(p []byte) (int, error) {
	dnw.mutex.Lock()
	defer dnw.mutex.Unlock()
	if dnw.Closed() {
		return 0, fmt.Errorf("dnw: closed")
	}

	return dnw.read(dnw.reader, p)
}
func (dnw *DNW) read(r *crunchio.Buffer, p []byte) (int, error) {
	return r.Read(p)
}
func (dnw *DNW) readThread() {
	dnw.buffer = crunchio.NewBuffer("EUB Writer", nil)
	dnw.buffer.SetStream(true) //Don't return an EOF when waiting on data
	dnw.reader = dnw.buffer.Reference()
	dnw.reader.SetName("EUB Reader")

	//Unlock the device's mutex to allow I/O operations
	dnw.mutex.Unlock()

	for {
		//Read the next chunk of data
		p := make([]byte, 10240)
		n, err := dnw.port.Read(p)
		if err != nil {
			break
		}
		if n == 0 {
			continue
		}

		//Write it to the message queue
		p = p[:n]
		_, err = dnw.buffer.Write(p)
		if err != nil {
			break
		}
	}

	dnw.close()
}

func (dnw *DNW) WriteCmd(cmd *Command) error {
	if cmd == nil {
		return fmt.Errorf("dnw: nil command")
	}
	return dnw.WriteMsg(NewMessage(cmd.Bytes()))
}
func (dnw *DNW) WriteMsg(msg *Message) error {
	dnw.mutex.Lock()
	defer dnw.mutex.Unlock()
	if dnw.Closed() {
		return fmt.Errorf("dnw: closed")
	}

	p := msg.Bytes()
	totalSize := len(p)

	fmt.Printf("dnw: writing complete message (%d bytes) in one operation\n", totalSize)
	fmt.Printf("dnw: message header (first 32 bytes): %X\n", p[:min(32, len(p))])
	fmt.Printf("dnw: message trailer (last 16 bytes): %X\n", p[max(0, len(p)-16):])

	// Check for large messages that might need special handling
	if totalSize > 20480 { // 20KB threshold
		fmt.Printf("dnw: WARNING - Large message detected (%d bytes)\n", totalSize)
		fmt.Printf("dnw: Device may have buffer limitations. Consider splitting at protocol level.\n")
	}

	// Write entire message at once - the protocol requires the complete packet
	// The serial driver will handle flow control internally
	n, err := dnw.writeChunked(p)
	if err != nil {
		return fmt.Errorf("dnw: failed to write message: %v", err)
	}
	if n != totalSize {
		return fmt.Errorf("dnw: only wrote %d/%d bytes", n, totalSize)
	}

	fmt.Printf("dnw: completed writing %d bytes\n", n)
	time.Sleep(time.Duration(100) * time.Millisecond)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (dnw *DNW) Write(p []byte) (int, error) {
	dnw.mutex.Lock()
	defer dnw.mutex.Unlock()
	if dnw.Closed() {
		return 0, fmt.Errorf("dnw: closed")
	}

	return dnw.write(p)
}
func (dnw *DNW) write(p []byte) (int, error) {
	n, err := dnw.port.Write(p)
	if err != nil {
		return n, fmt.Errorf("dnw: write error: %v", err)
	}
	
	// Critical: Wait for the serial port to actually transmit all data
	// This ensures the device's hardware buffer can handle the next chunk
	if err := dnw.port.Drain(); err != nil {
		return n, fmt.Errorf("dnw: Drain error: %v", err)
	}
	
	// Additional delay to allow device's RX buffer to process
	// Increased to 50ms for maximum reliability with large transfers
	time.Sleep(time.Duration(50) * time.Millisecond)
	
	return n, nil
}

// writeChunked writes data in small chunks with delays between them
// This avoids overwhelming the device's receive buffer while maintaining protocol integrity
func (dnw *DNW) writeChunked(p []byte) (int, error) {
	totalSize := len(p)
	chunkSize := 256 // Reduced to 256 for testing
	wrote := 0
	chunkNum := 0

	for wrote < totalSize {
		end := wrote + chunkSize
		if end > totalSize {
			end = totalSize
		}
		chunk := p[wrote:end]
		chunkNum++

		// Write chunk directly to port
		fmt.Printf("dnw: writing chunk #%d (%d bytes at offset %d/%d)\n", chunkNum, len(chunk), wrote, totalSize)
		n, err := dnw.port.Write(chunk)
		if err != nil {
			return wrote + n, fmt.Errorf("write error at offset %d: %v", wrote, err)
		}
		
		// Wait for data to be transmitted
		if err := dnw.port.Drain(); err != nil {
			return wrote + n, fmt.Errorf("drain error at offset %d: %v", wrote, err)
		}

		wrote += n
		
		// Check for device response after each chunk
		time.Sleep(time.Duration(20) * time.Millisecond)
		
		startPos, _ := dnw.reader.Seek(0, io.SeekCurrent)
		endPos, _ := dnw.buffer.Seek(0, io.SeekCurrent)
		available := endPos - startPos
		
		if available > 0 {
			responseData := make([]byte, available)
			rn, _ := dnw.reader.Read(responseData)
			if rn > 0 {
				fmt.Printf("dnw: !!! device sent %d bytes after chunk #%d: %q\n", rn, chunkNum, responseData[:rn])
				
				// Check if device sent NAK - if so, abort immediately
				if bytes.Contains(responseData[:rn], []byte("nak")) || bytes.Contains(responseData[:rn], []byte("NAK")) {
					return wrote, fmt.Errorf("device NAK'd after %d bytes - aborting transfer", wrote)
				}
			}
		}
		
		// Delay between chunks
		if wrote < totalSize {
			time.Sleep(time.Duration(30) * time.Millisecond)
		}
	}

	fmt.Printf("dnw: successfully wrote all %d chunks (%d total bytes)\n", chunkNum, wrote)
	return wrote, nil
}

func (dnw *DNW) Close() error {
	dnw.mutex.Lock()
	defer dnw.mutex.Unlock()
	return dnw.close()
}
func (dnw *DNW) close() error {
	if dnw.Closed() {
		return nil
	}
	if err := dnw.port.Close(); err != nil {
		return err
	}
	dnw.closed = true
	return nil
}
func (dnw *DNW) Closed() bool {
	return dnw.closed
}
func (dnw *DNW) Free() {
	dnw.close()
	dnw.buffer.Reset()
	dnw.buffer = nil
	dnw.reader = nil
	dnw.info = nil
}

func (dnw *DNW) GetBuffer() *crunchio.Buffer {
	if dnw.buffer == nil {
		return nil
	}
	return dnw.buffer.Reference()
}

func (dnw *DNW) GetPort() string {
	if dnw.Closed() {
		return ""
	}
	return dnw.info.Name
}
func (dnw *DNW) GetSerial() string {
	if dnw.Closed() {
		return ""
	}
	return dnw.info.SerialNumber
}
func (dnw *DNW) GetID() string {
	vid := dnw.GetVID()
	pid := dnw.GetPID()
	if vid == "" || pid == "" {
		return ""
	}
	return vid + ":" + pid
}
func (dnw *DNW) GetVID() string {
	if dnw.Closed() {
		return ""
	}
	return dnw.info.VID
}
func (dnw *DNW) GetPID() string {
	if dnw.Closed() {
		return ""
	}
	return dnw.info.PID
}
func (dnw *DNW) GetUSB() bool {
	if dnw.Closed() {
		return false
	}
	return dnw.info.IsUSB
}
