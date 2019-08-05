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
	for T :=0;T < 8 ;T++{
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
	this.node.PutOnRing(this.node.address,key,value)
	go func() {
		time.Sleep(500*time.Millisecond)
		this.node.PutOnRing(this.node.address,key,value)
	}()
	return true
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
	go this.fixFingersRoutine()
	go this.checkPredecessorRoutine()
	go this.backupRoutine()
	go this.checkTiedNodesRoutine()
}

func (this *Console)Create(){
}

func (this *Console)Join(address string) bool{
	//defer reportError("Join done.")
	time.Sleep(200 * time.Millisecond)
	err := this.node.join(address)
	if err!=nil {
		log.Println(err)
		err = this.node.join(address)
	}
	if err != nil{
		panic(err)
	}
	return true
}

func (this *Console)Quit(){
	//defer  reportError("Quit successfully.")
	quitNotifyRPC(this.node.address,this.node.backupAddr)
	this.Server.shutdown()
	this.node.quit()
	time.Sleep(time.Millisecond * 200)
}

func (this *Console)Ping(address string) bool{
	return checkValidRPC(address)
}

func ignoreError(){
	_ = recover()
}
/*Periodical routines*/
const intervalTime time.Duration = 150 * time.Millisecond

func (this *Console)stabilizeRoutine(){
	defer ignoreError()
	for{
		if this.Listening() == false{
			return
		}
		this.node.stabilize()
		time.Sleep(intervalTime)
	}
}

func (this *Console)checkPredecessorRoutine(){
	defer ignoreError()
	for{
		if this.Listening() == false{
			return
		}
		this.node.checkPredecessor()
		time.Sleep(intervalTime)
	}
}

func (this *Console)fixFingersRoutine(){
	defer ignoreError()
	for{
		if this.Listening() == false{
			return
		}
		this.node.fixFingers()
		time.Sleep(intervalTime)
	}
}

func (this *Console)backupRoutine(){
	defer ignoreError()
	for{
		if this.Listening() == false{
			return
		}
		this.node.checkBackup()
		time.Sleep(intervalTime)
	}
}


func (this *Console)checkTiedNodesRoutine(){
	defer ignoreError()
	for{
		if this.Listening() == false{
			return
		}
		this.node.checkResposibleNodes()
		time.Sleep(intervalTime)
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
	time.Sleep(time.Millisecond * 300)
}