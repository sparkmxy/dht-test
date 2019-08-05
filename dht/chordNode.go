/*
Implement a Class <ChordNode> with method described in the paper
Function <stabilize> <checkPredecessor> <fixFinger> and <fixSuccessor> need to run periodically
 */

package dht

import (
	"errors"
	"fmt"
	"math/big"
	"net/rpc"
	"strconv"
	"sync"
)

const successorListLen int = 100
const fingerN = 160;
var LocalIP string
type ChordNode struct {
	address string
	data map[string]string

	successor [successorListLen]string
	predecessor string
	finger [fingerN+1]string
	next int

	listening bool
	dataLock sync.RWMutex
	sucLock sync.RWMutex

	backup map[string]map[string]string
	backupAddr string
	backupLock sync.Mutex
}

func NewChordNode(_port int) *ChordNode{
	LocalIP = getLocalAddress()
	return &ChordNode{
		address : LocalIP + ":" + strconv.Itoa(_port),
		data : make(map[string]string),
		backup : make(map[string]map[string]string),
		next : 0,
	}
}

func (this *ChordNode) setPort(_port int){
	this.address = LocalIP + ":" + strconv.Itoa(_port);
}

func (this *ChordNode) create(){
	this.next = 0;
	for i:=0;i<successorListLen;i++{
		this.successor[i] = this.address
	}
	this.predecessor = ""
	this.listening = false
}

func (this *ChordNode)stabilize(){
	if !this.listening{
		return
	}
//	defer reportError("stabilize")
	suc := this.firstValidSuccessor()
	if suc == "" {
		fmt.Println(this.address,": stabilize: Successor does not exist.")
		return
	}

	client := getClinet(suc)
	if client == nil{
		return
	}
	pre := ""
	err := CallFunc(client,"ChordNode.GetPredecessor",1,&pre)
	if err != nil{
		fmt.Println(err)
		client.Close()
		return
	}
	if pre != "" && suc != pre && between(this.address,pre,suc,false){
		suc = pre
		client.Close()
		client = getClinet(suc)
	}
	defer client.Close()
	if suc == this.address{
		return ;
	}
	var temp [successorListLen]string
	err = CallFunc(client,"ChordNode.GetSuccessor",1,&temp)
	if err!=nil {
		fmt.Println(err)
		return
	}
	this.sucLock.Lock()
	for i:=successorListLen-1; i > 0 ;i--{
		this.successor[i] = temp[i-1]
	}
	this.successor[0] = suc
	//this.finger[1] = suc
	this.sucLock.Unlock()

	reply := false
	CallFunc(client,"ChordNode.Notify",this.address,reply)
	//notifyRPC(suc,this.address)
}


func (this *ChordNode)fixFingers(){
	if !this.listening{
		return
	}
	cnt := 1
	this.next++
	suc := this.firstValidSuccessor()
	temp := ""
	if suc != this.finger[1]{
		this.finger[1] = suc
		temp = suc
		this.next = 1
	}else {
		if this.next > fingerN {
			this.next = 1
		}
		err := this.FindSuccessorHelper(jump(this.address, this.next), &temp)
		if err != nil || temp == "" {
			//fmt.Println(this.address, ": ", err)
			this.next--
			return
		}
		this.finger[this.next] = temp
	}
	for{
		this.next++
		if this.next > fingerN{
			this.next = 1
		}
		if cnt <fingerN && hashBetween(hashString(this.address),jump(this.address,this.next),hashString(temp),true){
			cnt++
			this.finger[this.next] = temp
		}else{
			this.next--
			return
		}
	}
}



func (this *ChordNode)checkPredecessor(){
	if !this.listening{
		return
	}
//	defer reportError("checkPredecessor")
	if checkValidRPC(this.predecessor) == false{
		this.predecessor = ""
	}
}

func (this *ChordNode)join(address string)error{
	this.predecessor = ""
	suc,err:= findSuccessorRPC(address,hashString(this.address))
	if err == nil && suc != "" {
		var temp [successorListLen]string
		temp, err = successorRPC(suc)
		this.sucLock.Lock()
		if err == nil {
			for i := successorListLen - 1; i > 0; i-- {
				this.successor[i] = temp[i-1]
			}
			this.successor[0] = suc
		}
		// successor[0] must be valid now
		this.backupAddr = suc
		this.sucLock.Unlock()
		this.dataLock.Lock()
		this.data = divideRPC(suc, this.address)
		this.dataLock.Unlock()
		backupNotifyRPC(this.address,suc,this.data)
	}
	if suc == ""{
		return errors.New("join failed")
	}
	fmt.Println("Join: successor = ",suc)
	return err
}

func (this *ChordNode)closestPrecedingNode(hashAddr *big.Int) *rpc.Client{
	for i:= fingerN; i >0;{
		if this.finger[i] == "" {
			i--;
			continue;
		}
		if !hashBetween(hashString(this.address),hashString(this.finger[i]),hashAddr,false){
			i--
			continue
		}
		client := getClinet(this.finger[i])
		if client == nil {
			i--
			continue
		}
		reply := 0
		err := CallFunc(client,"ChordNode.Ping",1,&reply)
		if err == nil && reply == 1 {
			return client
		}else {
			client.Close()
			for j := i;i > 1 && this.finger[i] == this.finger[j];{
				i--
			}
		}
	}
	return getClinet(this.firstValidSuccessor())
}

func (this *ChordNode) firstValidSuccessor() string{
	this.sucLock.RLock()
	successors := this.successor
	this.sucLock.RUnlock()
	for _,suc := range successors{
		//fmt.Println("check: ",suc)
		if checkValidRPC(suc){
			return suc
		}
	}
	fmt.Println(this.address,":No valid successor is found.")
	return ""
}


func (this *ChordNode) quit(){
	suc := this.firstValidSuccessor()
	this.dataLock.Lock()
	mergeIntoRPC(suc,this.data)
	this.dataLock.Unlock()
}

func (this *ChordNode) Dumpself(){
	tempint := 0
	this.Dump(1,&tempint)
}

func (this *ChordNode) DumpAll(){
	this.Dumpself()
	for cur := this.successor[0];cur!=this.address;{
		fmt.Println("dumpAll now at ",cur)
		dumpRPC(cur)
		temp,_ := successorRPC(cur)
		cur = temp[0]
	}
}

/*RPC Services*/

var NotListeningError = errors.New("already closed")
func (this *ChordNode) GetData(request int, reply *map[string]string)error{
	if !this.listening{
		return NotListeningError
	}
	this.dataLock.RLock()
	*reply = this.data
	this.dataLock.RUnlock()
	return nil
}


type ReceiveT struct {
	data map[string]string
	address string
}
func (this *ChordNode)ReceiveData(dataset map[string]string, reply *int) error{
	if !this.listening{
		return NotListeningError
	}
	this.dataLock.Lock()
	for key,value := range dataset{
		if _,ok := this.data[key]; ok == false{
			*reply++
		}
		this.data[key] = value
	}
	this.dataLock.Unlock()
	temp := 0
	_ = Call(this.backupAddr,"ChordNode.ReceiveBackups",ReceiveT{dataset,this.address},temp)
	return nil
}

func (this *ChordNode)ReceiveBackups(request ReceiveT,reply *int) error{
	if !this.listening{
		return NotListeningError
	}
	this.backupLock.Lock()
	defer this.backupLock.Unlock()
	for key,value := range request.data{
		this.backup[request.address][key] = value
	}
	return nil
}

func (this *ChordNode) Ping(request int,reply *int) error{
	if this.listening {
		*reply = 1
	}else {
		return NotListeningError
	}
	return nil
}


func (this *ChordNode) GetPredecessor(request int,reply *string) error{
	if !this.listening{
		return NotListeningError
	}
	*reply = this.predecessor
	return nil
}

func (this *ChordNode) GetSuccessor(request int,reply *[successorListLen]string) error{
	if !this.listening{
		return NotListeningError
	}
	this.sucLock.RLock()
	*reply = this.successor
	this.sucLock.RUnlock()
	return nil
}

func (this *ChordNode) Notify(newPredecessor string,reply *bool) error{
	if !this.listening{
		return NotListeningError
	}
	if this.predecessor == ""{
		this.backupAddr = newPredecessor
		this.dataLock.RLock()
		backupNotifyRPC(this.address,this.backupAddr,this.data)
		this.dataLock.RUnlock()
	}
	if (this.predecessor == "") ||
		(between(this.predecessor,newPredecessor,this.address,false)){
		this.predecessor = newPredecessor
	}
	return nil
}

func (this *ChordNode) FindSuccessorHelper(hashAddr *big.Int,reply *string) error{
	if !this.listening{
		return NotListeningError
	}
	if suc := this.firstValidSuccessor(); hashBetween(hashString(this.address),hashAddr,hashString(suc),true){
		*reply = suc
		return nil
	} else {
		client := this.closestPrecedingNode(hashAddr)
		defer client.Close()
		err := CallFunc(client,"ChordNode.FindSuccessorHelper",hashAddr,reply)
		if err!=nil{
			return err
		}
		return nil
	}
}

func (this *ChordNode) GetValue(key string,reply *string)error{
	if !this.listening{
		return NotListeningError
	}
	this.dataLock.RLock()
	value,ok := this.data[key]
	if ok{
		*reply = value
	}else{
		*reply = ""
	}
	this.dataLock.RUnlock()
	return nil
}

func (this *ChordNode) Put(pair [2]string,reply *bool) error{
	if !this.listening{
		return NotListeningError
	}
	this.dataLock.Lock()
	this.data[pair[0]] = pair[1]
	*reply = true;
	this.dataLock.Unlock()
	putBackupRPC(pair,this.address,this.backupAddr)
	return nil
}

func (this *ChordNode) Delete(key string,reply *string) error{
	if !this.listening{
		return NotListeningError
	}
	this.dataLock.Lock()
	value,ok := this.data[key]
	if ok {    //Delete successfully
		*reply = value
		delete(this.data,key)
		this.dataLock.Unlock()
		deleteBackupRPC(key,this.address,this.backupAddr)
		return nil
	} else{
		this.dataLock.Unlock()
		return errors.New("Delete: Key does not exist.")
	}
}

func (this *ChordNode) Divide(address string,reply *map[string]string)error{
	if !this.listening{
		return NotListeningError
	}
	this.dataLock.RLock()
	for key,value := range this.data{
		if between(address,key,this.address,true) == false{
			delete(this.data,key)
			(*reply)[key] = value
		}
	}
	this.dataLock.RUnlock()
	return nil
}


/*Other functions*/
func (this *ChordNode) Dump(request int,reply *int)error{
	this.dataLock.RLock()
	this.sucLock.RLock()
	fmt.Println("------------------------dump:",this.address,"--------------------------")
	fmt.Println("successor: ",this.successor)
	fmt.Println("hash: ",hashString(this.address))
	fmt.Println("data: ",this.data)
	fmt.Println("fingers: ",this.finger)
	fmt.Println("backups: ",this.backup)
	fmt.Println("backupAddress: ",this.backupAddr)
	fmt.Println("----------------------------------------------------------------------------")
	this.sucLock.RUnlock()
	this.dataLock.RUnlock()
	return nil
}

/*Functions for backup*/
func (this *ChordNode) BackupNotify(para ParaType,reply *bool) error{
	if !this.listening{
		return NotListeningError
	}
	this.backupLock.Lock()
	this.backup[para.Address] = para.Data
	this.backupLock.Unlock()
	return nil
}

func (this *ChordNode) PutBackup(pair [3]string, reply *bool) error{
	if !this.listening{
		return NotListeningError
	}
	// fmt.Println(this.address,": PutBackup: from ",pair[0])
	this.backupLock.Lock()
	if this.backup[pair[0]] == nil{
		this.backup[pair[0]] = make(map[string]string)
	}
	this.backup[pair[0]][pair[1]] = pair[2]
	this.backupLock.Unlock()
	return nil
}

func (this *ChordNode) DeleteBackup(pair [2]string, reply *bool) error{
	if !this.listening{
		return NotListeningError
	}
	this.backupLock.Lock()
	delete(this.backup[pair[0]],pair[1])
	this.backupLock.Unlock()
	return nil;
}

func (this *ChordNode)checkBackup(){
	if !this.listening{
		return
	}
	if checkValidRPC(this.backupAddr){
		return;
	}
	newBackup := this.firstValidSuccessor()
	//this.dataLock.RLock()
	backupNotifyRPC(this.address,newBackup,this.data)
	//this.dataLock.RUnlock()
	this.backupAddr = newBackup
}

func (this *ChordNode)checkResposibleNodes(){
	if !this.listening{
		return
	}
	for address,_ := range this.backup{
		if address == ""{
			continue
		}
		if checkValidRPC(address) == false{
			this.backupLock.Lock()
			fmt.Println(this.address,"Try to revert node ",address)
			this.putPairs(address,this.backup[address])
			delete(this.backup,address)
			this.backupLock.Unlock()
		}else {
			if getBackupAddressRPC(address) != this.address{
				this.backupLock.Lock()
				delete(this.backup,address)
				this.backupLock.Unlock()
			}
		}
	}
}

func (this *ChordNode) Untie(address string,reply *bool) error{
	if !this.listening{
		return NotListeningError
	}
	this.backupLock.Lock()
	delete(this.backup,address)
	this.backupLock.Unlock()
	return nil
}

func (this *ChordNode) putPairs(address string,data map[string]string){
	suc := ""
	err := this.FindSuccessorHelper(hashString(address),&suc)
	if err != nil{
		fmt.Println("putPairs：An error occur when finding successor: ",err)
	}
	reply := 0;
	_ = Call(suc,"ChordNode.ReceiveData",data,&reply)
	fmt.Println(reply," of ",len(data)," items are received at ",suc,".")

}

func (this *ChordNode) GetBackupAddress(request int, reply *string) error{
	if !this.listening{
		return NotListeningError
	}
	*reply = this.backupAddr
	return nil
}