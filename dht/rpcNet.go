package dht

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

type Server struct{
	server *rpc.Server
	listener net.Listener
	node *ChordNode
}

func NewServer(o *ChordNode) *Server{
	return &Server{node : o}
}

func (s *Server)Launch() error{
	s.server = rpc.NewServer()
	if err := s.server.Register(s.node); err != nil {
		fmt.Println("Register failed: ",err)
		return err
	}

	lsn,err := net.Listen("tcp",s.node.address)
	if err!=nil{
		fmt.Println("Listen failed.")
		return err
	}
	s.listener = lsn
	s.node.create()
	s.node.listening = true

	go s.server.Accept(s.listener)

	return nil
}

func (s *Server) shutdown(){
	if err:=s.listener.Close(); err != nil{
		log.Println(err)
		//panic(err)
	}
	s.node.listening = false;
	fmt.Println(s.node.address,":shutdown ok.")
}

/*RPC-Callings*/
func Call(address string,method string,request interface{}, reply interface{}) error{
	if address == ""{
		log.Println(method + ": Address is empty.")
		return errors.New("Calling to an empty address.")
	}
	//time.Sleep(time.Duration(rand.Int()%100+10))
	client,err := rpc.Dial("tcp",address)

	if err != nil{
		return err
	}
	err = client.Call(method,request,reply)
	client.Close()
	if err != nil{
		return err;
	}
	return nil;
}

func checkValidRPC(address string) bool{
	if address == ""{
		return false
	}
	ch := make(chan bool)
	for T:=0;T<2;T++{
		go func() {
			reply := 0
			err := Call(address,"ChordNode.Ping",1,&reply)
			if err == nil && reply == 1 {
				ch <- true
			}else{
				ch <- false
			}
		}()
		select {
			case ok := <- ch :
				if ok{
					return true
				}
			case <-time.After(200 * time.Millisecond):
				//fmt.Println("Ping ",address, " time out")
		}
	}
	return false
}

func predecessorRPC(address string)(string,error){
	reply := ""
	if err := Call(address,"ChordNode.GetPredecessor",1,&reply); err != nil{
		return "",err
	}
	/*
	if reply == ""{
		return "",errors.New("No predecessor")
	}
	*/
	return reply,nil
}

func successorRPC(address string)([successorListLen]string,error){
	reply := [successorListLen]string{}
	if err := Call(address,"ChordNode.GetSuccessor",1,&reply); err != nil{
		return [successorListLen]string{},err
	}
	if reply == [successorListLen]string{}{
		return [successorListLen]string{},errors.New("No successor")
	}
	return reply,nil
}

func notifyRPC(address string,newPredecessor string)error{
	reply := false;
	return Call(address,"ChordNode.Notify",newPredecessor,&reply)
}

func (this *ChordNode)FindSuccessor(nodeAddress string,hashAddr *big.Int) (string,error){
	reply := ""
	if err:= this.FindSuccessorHelper(hashAddr,&reply);err!=nil{
		return "",err
	}
	return reply,nil
}

func findSuccessorRPC(nodeAddress string,hashAddr *big.Int) (string,error){
	reply := ""
	/*
	if err := Call(nodeAddress, "ChordNode.FindSuccessorHelper", hashAddr, &reply); err != nil {
		return "",err
	}

	 */

	var err error = nil
	ch := make(chan bool)
	for T:=0;T<2;T++ {
		go func() {
			reply = ""
			if err = Call(nodeAddress, "ChordNode.FindSuccessorHelper", hashAddr, &reply); err != nil {
				ch <- false
			}else{
				ch <- true
			}
		}()
		select {
			case ok := <- ch:
				if ok {
					return reply,nil
				}
			case <- time.After(500*time.Millisecond):
		}
	}

	return reply,nil
}

var TimeoutError error = errors.New("TIme out")

func (this *ChordNode)Find(address string,key string)string{
	ch := make(chan bool)
	suc := "";
	var err error = nil
	for T:=0;T<2;T++{
		go func() {
			suc,err = this.FindSuccessor(address,hashString(key))
			if err != nil{
				ch <- false
			}else{
				ch <- true
			}
		}()
		select {
		case ok := <- ch:
			if ok{
				break
			}

		case <-time.After(500*time.Millisecond):
			err = TimeoutError
		}
	}
	if err != nil{
		fmt.Println("findRPC：An error occur when finding successor.")
		return ""
	}
	value := ""
	done := make(chan bool)
	go func() {
		err = Call(suc,"ChordNode.GetValue",key,&value)
		done <- true

	}()
	select {
	case <- done:
		return value
	case <-time.After(200 *time.Millisecond):
		return ""
	}

	return value
}

func (this *ChordNode) PutOnRing(address string,key string,value string) bool {
	suc,err := this.FindSuccessor(address,hashString(key))
	if err != nil{
		fmt.Println("putRPC：An error occur when finding successor: ",err)
	}
	reply := false;
	err = Call(suc,"ChordNode.Put",[2]string{key,value},&reply)
	if reply{
		fmt.Printf("put <%s,%s> at %s\n",key ,value,suc)
	}
	return reply
}

func (this *ChordNode) DeleteOnRing(address string,key string) (string,bool){
	suc,err := this.FindSuccessor(address,hashString(key))
	if err != nil{
		fmt.Println("deleteRPC：An error occur when finding successor: ",err )
	}
	ret := ""
	err = Call(suc,"ChordNode.Delete",key,&ret)
	if err == nil{
		return ret,true
	}
	return "",false
}

func divideRPC(address string,prev string) map[string]string{
	ret := make(map[string]string)
	err := Call(address,"ChordNode.Divide",prev,&ret)
	if err != nil{

	}
	return ret
}

func mergeIntoRPC(address string,dataset map[string]string) {
	reply := 0
	err:= Call(address,"ChordNode.ReceiveData",dataset,&reply)
	if tot:=len(dataset);reply != tot{
		fmt.Printf("tot items: %d, %d items are successfully merged into %s\n",tot,reply,address)
	}
	if(err!=nil){
		fmt.Println(err)
	}
}

func dumpRPC(address string) map[string]string{
	reply := make(map[string]string)
	_ = Call(address, "ChordNode.Dump", 1, &reply)
	return reply
}

type ParaType struct {
	Address string
	Data map[string]string
}

func backupNotifyRPC(address ,backupAddr string,data map[string]string) {
	reply := false
	_ = Call(backupAddr,"ChordNode.BackupNotify",ParaType{address,data},&reply)
}

func quitNotifyRPC(address string,backup string){
	if backup == ""{
		return
	}
	reply := false
	_ = Call(backup,"ChordNode.Untie",address,&reply)
}

func putBackupRPC(pair [2]string,address string,backup string){
	if backup == ""{
		return
	}
	reply := false
	_ = Call(backup,"ChordNode.PutBackup",[3]string {address,pair[0],pair[1]},&reply)
}

func deleteBackupRPC(key,address,backup string){
	if backup == ""{
		return
	}
	reply := false
	err := Call(backup,"ChordNode.DeleteBackup",[2]string{address,key},&reply)
	if err != nil{
		fmt.Println(err)
	}
}

func getBackupAddressRPC(address string) string{
	reply := ""
	err := Call(address,"ChordNode.GetBackupAddress",1,&reply)
	if err != nil{
		fmt.Println(err)
	}
	return reply
}
