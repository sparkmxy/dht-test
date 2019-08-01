
package dht

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
)

func getLocalAddress() string{
	var localAddress string
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}
	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}
			for _, addr := range addrs {
				if ipNet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipNet.IP.To4(); len(ip4) == net.IPv4len{
						localAddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localAddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localAddress
}

func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

const keySize = sha1.Size * 8
var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize),
	nil)
func jump(address string, fingerEntry int) *big.Int {
	n := hashString(address)
	fingerEntryMinus1 := big.NewInt(int64(fingerEntry) - 1)
	jump := new(big.Int).Exp(two, fingerEntryMinus1, nil)
	sum := new(big.Int).Add(n, jump)
	return new(big.Int).Mod(sum, hashMod)
}

func hashBetween(start,elt,end *big.Int,inclusive bool) bool {
	if end.Cmp(start) > 0{
		return (start.Cmp(elt) < 0 && end.Cmp(elt) > 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || end.Cmp(elt) > 0 || (inclusive && elt.Cmp(end)==0)
	}
}

func between(start,elt,end string,inclusive bool) bool{
	return hashBetween(hashString(start),hashString(elt),hashString(end),inclusive)
}

func reportError(msg string){
	if err:=recover(); err!=nil{
		fmt.Println("Oops! ",err)
	}else{
		if msg == ""{
			msg = "No error."
		}
		fmt.Println(msg)
	}
}
