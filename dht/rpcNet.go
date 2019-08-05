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
		// try again
		lsn,err = net.Listen("tcp",s.node.address)
		if err != nil {
			return err
			fmt.Println("Listen failed.")
		}
	}
	s.listener = lsn
	s.node.create()
	s.node.listening = true

	go s.server.Accept(s.listener)
	return nil
}

func (s *Server) shutdown(){
	s.node.listening = false;
	go func() {
		time.Sleep(400 * time.Millisecond)
		if err:=s.listener.Close(); err != nil{
			log.Println(err)
			//panic(err)
		}
	}()
	fmt.Println(s.node.address,":shutdown ok.")
}


/*RPC-Callings*/
/*
func Call(address string,method string,request interface{}, reply interface{}) error{
	if address == ""{
		log.Println(method + ": Address is empty.")
		return errors.New("Calling to an empty address.")
	}
	//time.Sleep(time.Duration(rand.Int()%100+10))
	client,err := rpc.Dial("tcp",address)
	done := make(chan struct{})
	defer func() {
		if client != nil{
			client.Close()
		}
	}()
	if err != nil{
		return err
	}

	go func() {
		err = client.Call(method,request,reply)
		done <- struct{}{}
	}()
	select {
	case <- done:
	case <- time.After(3 * time.Second):
		err = TimeoutError
	}
	if err != nil{
		return err;
	}
	return nil;
}

 */

var emptyAddressError error = errors.New("Calling to an empty address")

func getClinet(address string) *rpc.Client{
	if address == ""{
		log.Println("Address is empty.")
		return nil
	}
	//time.Sleep(time.Duration(rand.Int()%100+10))
	conn, err := net.DialTimeout("tcp",address,1 * time.Second)
	if err != nil{
		return nil
	}
	err = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil{
		_  = conn.Close()
		return nil
	}
	return rpc.NewClient(conn)
}

func CallFunc(client *rpc.Client,method string, request interface{},reply interface{}) error{
	var err error
	select {
	case call := <- client.Go(method,request,reply,make(chan *rpc.Call,1)).Done:
		err = call.Error
	case <- time.After(1*time.Second):
		err = TimeoutError
	}
	return err
}

func Call(address string,method string,request interface{}, reply interface{}) error{
	if address == ""{
		log.Println(method + ": Address is empty.")
		return emptyAddressError
	}
	//time.Sleep(time.Duration(rand.Int()%100+10))
	conn, err := net.DialTimeout("tcp",address,1 * time.Second)
	if err != nil{
		return err
	}

	err = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err != nil{
		_  = conn.Close()
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	select {
		case call := <- client.Go(method,request,reply,make(chan *rpc.Call,1)).Done:
			err = call.Error
		case <- time.After(1*time.Second):
			err = TimeoutError
	}
	if err != nil{
		return err
	}
	return nil
}

func checkValidRPC(address string) bool{
	if address == ""{
		return false
	}

	reply := 0
	err := Call(address,"ChordNode.Ping",1,&reply)

	if err != nil || reply != 1{
		err = Call(address,"ChordNode.Ping",1,&reply)
	}
	return err == nil && reply == 1
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

func findSuccessorRPC(nodeAddress string,hashAddr *big.Int)(string,error){
	reply := ""

	if err := Call(nodeAddress, "ChordNode.FindSuccessorHelper", hashAddr, &reply); err != nil {
		return "",err
	}

	return reply,nil
}


var TimeoutError error = errors.New("TIme out")

func (this *ChordNode)Find(address string,key string)string{
	suc := "";
	var err error = nil
	suc,err = this.FindSuccessor(address,hashString(key))

	if err != nil{
		//fmt.Println("findRPC：An error occur when finding successor.")
		return ""
	}
	value := ""
	err = Call(suc,"ChordNode.GetValue",key,&value)
	if err == nil{
		return value
	}
	return ""
}

func (this *ChordNode) PutOnRing(address string,key string,value string) bool {
	suc,err := this.FindSuccessor(address,hashString(key))
	if err != nil {
		//fmt.Println("putRPC：An error occur when finding successor: ",err)
		return false
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
