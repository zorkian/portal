/*
 * this is portal.
 *
 * this software provides replication of Kafka data from one cluster to another. this is
 * designed to be used in larger infrastrutures where you might be running portal on
 * a number of machines, so it should handle heavy loads.
 *
 * see the README and LICENSE.
 *
 */

package main

import (
	"../marshal"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("Portal")

func main() {
	/*	format := logging.MustStringFormatter(
			"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
		)
		logging.SetFormatter(format)
	*/

	marsh, err := marshal.NewMarshaler([]string{"127.0.0.1:9092"})
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	log.Info("Starting up! %s", marsh)
}
