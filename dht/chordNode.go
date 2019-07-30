/*
Implement a Class <ChordNode> with method described in the paper
Function <stabilize> <checkPredecessor> <fixFinger> and <fixSuccessor> need to run periodically
 */

package dht

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"sync"
)

const successorListLen int = 8
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
//	defer reportError("stabilize")
	suc := this.firstValidSuccessor()
	if suc == "" {
		fmt.Println(this.address,": stabilize: Successor does not exist.")
		return
}
	pre,err:= predecessorRPC(suc)
	if err != nil{
		fmt.Println(err)
		return
	}
	if pre != "" && between(this.address,pre,suc,false){
		suc = pre
	}
	if suc == this.address{
		return ;
	}
	temp,err1 := successorRPC(suc)
	if err1!=nil {
		fmt.Println(err1)
		return
	}
	this.sucLock.Lock()
	for i:=successorListLen-1; i > 0 ;i--{
		this.successor[i] = temp[i-1]
	}
	this.successor[0] = suc
	//this.finger[1] = suc
	this.sucLock.Unlock()

	if suc == ""{
		fmt.Println("stabilize: successor is nil")
		return
	}
	notifyRPC(suc,this.address)
}


func (this *ChordNode)fixFingers(){
	//	defer reportError("fixFingers")
	/*
		this.next++
		if this.next > fingerN{
			this.next = 1
		}
		temp := ""
		if err := this.FindSuccessor(jump(this.address,this.next),&temp); err == nil && temp!= ""{
			this.finger[this.next] = temp;
		}
	*/
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
			fmt.Println(this.address, ": ", err)
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
//	defer reportError("checkPredecessor")
	if checkValidRPC(this.predecessor) == false{
		this.predecessor = ""
	}
}

func (this *ChordNode)join(address string)error{
	this.predecessor = ""
	suc,err:= findSuccessorRPC(address,hashString(this.address))
	if err == nil {
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
		this.data = divideRPC(suc, this.address)
		backupNotifyRPC(this.address,suc,this.data)
	}
	fmt.Println("Join: successor = ",suc)
	return err
}

func (this *ChordNode)closestPrecedingNode(hashAddr *big.Int) string{
	for i:= fingerN; i >0;{
		if this.finger[i] == "" {
			i--;
			continue;
		}
		if hashBetween(hashString(this.address),hashString(this.finger[i]),hashAddr,false) && checkValidRPC(this.finger[i]){
			return this.finger[i]
		}else {
			for j := i;i > 1 && this.finger[i] == this.finger[j];{
				i--
			}
		}
	}
	return this.address
}

func (this *ChordNode) firstValidSuccessor() string{
	this.sucLock.RLock()
	defer this.sucLock.RUnlock()
	for _,suc := range this.successor{
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

func (this *ChordNode) GetData(request int, reply *map[string]string)error{
	this.dataLock.RLock()
	*reply = this.data
	this.dataLock.RUnlock()
	return nil
}

func (this *ChordNode)ReceiveData(dataset map[string]string, reply *int) error{
	this.dataLock.Lock()
	for key,value := range dataset{
		if _,ok := this.data[key]; ok == false{
			*reply++
		}
		this.data[key] = value
	}
	this.dataLock.Unlock()
	return nil
}

func (this *ChordNode) Ping(request int,reply *int) error{
	*reply = 1
	return nil
}


func (this *ChordNode) GetPredecessor(request int,reply *string) error{
	*reply = this.predecessor
	return nil
}

func (this *ChordNode) GetSuccessor(request int,reply *[successorListLen]string) error{
	this.sucLock.RLock()
	*reply = this.successor
	this.sucLock.RUnlock()
	return nil
}

func (this *ChordNode) Notify(newPredecessor string,reply *bool) error{
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
	if suc := this.firstValidSuccessor(); hashBetween(hashString(this.address),hashAddr,hashString(suc),true){
		*reply = suc
		return nil
	} else {
		x := this.closestPrecedingNode(hashAddr)
		ret,err := findSuccessorRPC(x,hashAddr)
		*reply = ret
		if err!=nil{
			return err
		}
		return nil
	}
}

func (this *ChordNode) GetValue(key string,reply *string)error{
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
	this.dataLock.Lock()
	this.data[pair[0]] = pair[1]
	*reply = true;
	this.dataLock.Unlock()
	go putBackupRPC(pair,this.address,this.backupAddr)
	return nil
}

func (this *ChordNode) Delete(key string,reply *string) error{
	this.dataLock.Lock()
	value,ok := this.data[key]
	if ok {    //Delete successfully
		*reply = value
		delete(this.data,key)
		this.dataLock.Unlock()
		go deleteBackupRPC(key,this.address,this.backupAddr)
		return nil
	} else{
		this.dataLock.Unlock()
		return errors.New("Delete: Key does not exist.")
	}
}

func (this *ChordNode) Divide(address string,reply *map[string]string)error{
	this.dataLock.Lock()
	for key,value := range this.data{
		if between(address,key,this.address,true) == false{
			delete(this.data,key)
			(*reply)[key] = value
		}
	}
	this.dataLock.Unlock()
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
	this.backup[para.Address] = para.Data
	return nil
}

func (this *ChordNode) PutBackup(pair [3]string, reply *bool) error{
	fmt.Println(this.address,": PutBackup: from ",pair[0])
	this.backupLock.Lock()
	if this.backup[pair[0]] == nil{
		this.backup[pair[0]] = make(map[string]string)
	}
	this.backup[pair[0]][pair[1]] = pair[2]
	this.backupLock.Unlock()
	return nil
}

func (this *ChordNode) DeleteBackup(pair [2]string, reply *bool) error{
	this.backupLock.Lock()
	delete(this.backup[pair[0]],pair[1])
	this.backupLock.Unlock()
	return nil;
}

func (this *ChordNode)checkBackup(){
	if checkValidRPC(this.backupAddr){
		return;
	}
	newBackup := this.firstValidSuccessor()
	this.dataLock.Lock()
	backupNotifyRPC(this.address,newBackup,this.data)
	this.dataLock.Unlock()
	this.backupAddr = newBackup
}

func (this *ChordNode)checkResposibleNodes(){
	for address,_ := range this.backup{
		if address == ""{
			continue
		}
		if checkValidRPC(address) == false{
			fmt.Println(this.address,"Try to revert node ",address)
			this.putPairs(address,this.backup[address])
			delete(this.backup,address)
		}else {
			if getBackupAddressRPC(address) != this.address{
				delete(this.backup,address)
			}
		}
	}
}

func (this *ChordNode) Untie(address string,reply *bool) error{
	delete(this.backup,address)
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
	*reply = this.backupAddr
	return nil
}