package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	BotiMinim     = 20
	DepositsQueue = "Depositos"
)

var totalBalance int

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError("Failed to connect to RabbitMQ", err)
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError("Failed to open a channel", err)
	defer ch.Close()

	q := declararCola(ch, DepositsQueue)

	fmt.Println("El tresorer és al despatx. El botí mínim és: " + strconv.Itoa(BotiMinim))

	msgs := recibirDepositos(ch, q)

	processOperacio(ch, msgs)
}

func recibirDepositos(ch *amqp.Channel, q amqp.Queue) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError("Failed to register a consumer", err)
	return msgs
}

func processOperacio(ch *amqp.Channel, msgs <-chan amqp.Delivery) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		// Recibir el mensaje
		d := <-msgs

		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

		// El mensaje tiene la estructura "operacion|nombreCliente". Hay que separarlo
		message := strings.Split(string(d.Body), "|")

		operacion, error := strconv.Atoi(message[0])
		failOnError("Failed to convert string to int", error)

		nombreCliente := message[1]
		fmt.Printf("Operació rebuda: %d del client: %s\n", operacion, nombreCliente)

		// Si la suma o resta del balance y la operación es mayor que 0, se puede realizar la operación
		if totalBalance+operacion >= 0 {
			totalBalance += operacion
		} else {
			fmt.Println("OPERACIO NO PERMESA NO HI HA FONS")
		}

		fmt.Printf("Balanç: %d\n", totalBalance)

		// En el caso de que el balance sea mayor que el botiMinim, se envía un balance de 0, ya que roba el botín
		balanceAEnviar := totalBalance
		if totalBalance >= BotiMinim {
			balanceAEnviar = 0
		}

		// Enviar el balance
		err := ch.PublishWithContext(ctx,
			"",        // intercambio
			d.ReplyTo, // clave de enrutamiento
			false,     // obligatorio
			false,     // inmediato
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(strconv.Itoa(balanceAEnviar)),
			},
		)
		failOnError("Failed to publish a message", err)

		time.Sleep(1 * time.Second)

		if totalBalance >= BotiMinim {
			fmt.Println("El tresorer decideix robar el diposit i tancar el despatx")
			fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
			break
		}
		fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	}

	err := ch.PublishWithContext(ctx,
		"amq.fanout", // intercambio
		"",           // clave de enrutamiento
		false,        // obligatorio
		false,        // inmediato
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("L'oficina acaba de tancar"),
		},
	)
	failOnError("Failed to publish a message", err)

	fmt.Println("El Tresorer se'n va\n")

	// Borra las colas del sistema
	_, err = ch.QueueDelete(DepositsQueue, false, false, false)
	failOnError("Failed to delete a queue", err)
	fmt.Printf("Cua %s esborrada amb èxit", DepositsQueue)

}

func declararCola(ch *amqp.Channel, name string) amqp.Queue {
	q, err := ch.QueueDeclare(
		name,  // nombre de la cola
		false, // duradero
		false, // autoeliminable
		false, // exclusivo
		false, // noWait
		nil,   // argumentos
	)
	failOnError("Failed to declare a queue", err)

	err = ch.QueueBind(
		q.Name,       // nombre de la cola
		"",           // clave de enrutamiento
		"amq.fanout", // intercambio
		false,        // noWait
		nil,          // argumentos
	)
	failOnError("Failed to bind a queue", err)

	return q
}

func failOnError(message string, err error) {
	if err != nil {
		log.Fatalf("%s: %s", message, err)
		panic(fmt.Sprintf("%s: %s", message, err))
	}
}
