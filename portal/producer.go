/*
 * portal - producer
 *
 * produces data to a local cluster. operates by taking data from a consumer and producing
 * it synchronously (with proper batching) and as soon as the data is produced it tells the
 * consumer to commit the offset.
 *
 */

package main
