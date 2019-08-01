package dht

import (
	"fmt"
	"log"
	"time"
)

type Console struct {
	node *ChordNode
	Server *Server
}

func (this *Console) SetPort(port int){
	this.node = NewChordNode(port)
	this.node.setPort(port)
	this.node.create()
}

/*Implement the interface <dhtNode>*/
func (this *Console)Get(key string) (bool,string){
	if this.node.listening == false{
		fmt.Println("Offline.")
		return false,""
	}
	for T :=0;T < 3 ;T++{
		value := this.node.Find(this.node.address,key)
		if value != ""{
			fmt.Printf("get : <%s,%s>\n",key,value)
			return true,value
		}
	}
	log.Println("Not found.")
	return false,""
}

func (this *Console)Put(key,value string) bool{
	if this.node.listening == false{
		fmt.Println("Offline.")
		return false
	}
	for T:=0;T < 3 ;T++ {
		if this.node.PutOnRing(this.node.address,key,value) {
			return true
		}
	}
	return false
}

func (this *Console)Del(key string) bool{
	if this.node.listening == false{
		fmt.Println("Offline.")
		return false
	}
	value,ok := this.node.DeleteOnRing(this.node.address,key)
	fmt.Println("Delete: ",value)
	return ok
}

func (this *Console)Run(){
	defer reportError("Launch successfully.")
	this.Server = NewServer(this.node)
	this.Server.Launch()
	go this.stabilizeRoutine()
	go this.checkPredecessorRoutine()
	go this.fixFingersRoutine()
	go this.checkPredecessorRoutine()
	go this.checkTiedNodesRoutine()
}

func (this *Console)Create(){
}

func (this *Console)Join(address string) bool{
	//defer reportError("Join done.")
	err := this.node.join(address)
	return err == nil
}

func (this *Console)Quit(){
	//defer  reportError("Quit successfully.")
	quitNotifyRPC(this.node.address,this.node.backupAddr)
	this.Server.shutdown()
	this.node.quit()
}

func (this *Console)Ping(address string) bool{
	return checkValidRPC(address)
}

func ignoreError(){
	_ = recover()
}
/*Periodical routines*/
const intervalTime time.Duration = 300 * time.Millisecond

func (this *Console)stabilizeRoutine(){
	defer ignoreError()
	ticker := time.Tick(intervalTime)
	for{
		if this.Listening() == false{
			return
		}
		select {
			case <-ticker:
				this.node.stabilize()
		}
	}
}

func (this *Console)checkPredecessorRoutine(){
	defer ignoreError()
	ticker := time.Tick(intervalTime)
	for{
		if this.Listening() == false{
			return
		}
		select {
			case <-ticker:
				this.node.checkPredecessor()
		}
	}
}

func (this *Console)fixFingersRoutine(){
	defer ignoreError()
	ticker := time.Tick(intervalTime)
	for{
		if this.Listening() == false{
			return
		}
		select {
		case <-ticker:
			this.node.fixFingers()
		}
	}
}

func (this *Console)backupRoutine(){
	defer ignoreError()
	ticker := time.Tick(intervalTime)
	for{
		if this.Listening() == false{
			return
		}
		select {
		case <-ticker:
			this.node.checkBackup()
		}
	}
}

func (this *Console)checkTiedNodesRoutine(){
	defer ignoreError()
	ticker := time.Tick(intervalTime)
	for{
		if this.Listening() == false{
			return
		}
		select {
		case <-ticker:
			this.node.checkResposibleNodes()
		}
	}
}

/*Other functions*/


func (this *Console)Listening() bool {
	return this.node.listening
}

func (this *Console) Dump(){
	this.node.Dumpself()
}

func (this *Console) DumpAll(){
	this.node.DumpAll()
}

func (this *Console) ForceQuit(){
	this.Server.shutdown()
}