package main

import "dht"

func NewNode(port int) dhtNode{
	console := dht.Console{}
	console.SetPort(port)
	var ret dhtNode = &console
	return ret
}