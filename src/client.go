package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DepositsQueue = "Depositos"
	BalancesQueue = "Balances"
)

var nombre string

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <nombre>")
		os.Exit(1)
	}
	nombre = os.Args[1]

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError("Failed to connect to RabbitMQ", err)
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError("Failed to open a channel", err)
	defer ch.Close()

	q := declararCola(ch, DepositsQueue)

	fanoutQueue := declararFanoutCola(ch)

	// Creamos consumidor del fanout
	msgsfanout := crearConsumir(ch, fanoutQueue)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// El número de operaciones a realizar será aleatorio entre 1 y 5
	numOperaciones := rand.Intn(5) + 1

	fmt.Println("Hola, el meu nom es: " + nombre)
	fmt.Println(nombre + " vol fer " + strconv.Itoa((numOperaciones)) + " operacions")

	// Creamos un hilo paralelo que ira buscando el mensaje de fanout del tresorer
	go func() {

		for d := range msgsfanout {
			fmt.Printf("El Tesorer ha dit: %s\n", d.Body)
			fmt.Println("Jo també me'n vaig idò!")
			os.Exit(0)
		}

	}()

	// Realizamos las operaciones
	for aux := 0; aux < numOperaciones; aux++ {

		// El mensaje vendrá de la forma "operacion|nombre", hay que separarlo
		operacio := generarOperacio()
		fmt.Printf("%s operació %d: %d\n", nombre, aux+1, operacio)
		message := strconv.Itoa(operacio) + "|" + nombre

		enviarOperacio(ch, operacio, message, q, ctx)

		fmt.Printf("%d-----------------------------\n", aux+1)
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)

	}

}

// Función que envía la operación al tresorer
func enviarOperacio(ch *amqp.Channel, operacio int, message string, q amqp.Queue, ctx context.Context) {

	// Creamos la cola de respuesta del tresorer
	replyQueue := declararCola(ch, "")
	msgs := crearConsumir(ch, replyQueue)

	// Enviamos la operación al tresorer
	err := ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			ReplyTo:     replyQueue.Name,
			Body:        []byte(message),
		},
	)
	failOnError("Failed to publish a message", err)

	fmt.Println("Operació sol·licitada")

	// Tiempo de espera para simular el tiempo de la operación
	time.Sleep(time.Duration(500) * time.Millisecond)

	// Recibimos la respuesta del tresorer, con el balance
	d := <-msgs

	balance, error := strconv.Atoi(string(d.Body))
	failOnError("Failed to convert string to int", error)
	imprimirMensajeOperacion(operacio, balance)

	fmt.Printf("Balanç actual: %d\n", balance)
}

func imprimirMensajeOperacion(operacio, balance int) {
	if operacio > 0 {
		if balance == 0 {
			fmt.Println("EL TESORER SEMBLA SOSPITÓS")
		} else {
			fmt.Println("INGRÉS CORRECTE")
		}
	} else {
		if balance == 0 {
			fmt.Println("NO HI HA SALDO AL COMPTE")
		} else {
			fmt.Println("ES FARÀ EL REINTEGRE SI HI HA SALDO")
		}
	}
}

func declararCola(ch *amqp.Channel, nombre string) amqp.Queue {
	q, err := ch.QueueDeclare(
		nombre, // nombre de la cola
		false,  // duradero
		false,  // autoeliminable
		false,  // exclusivo
		false,  // noWait
		nil,    // argumentos
	)
	failOnError("Failed to declare a queue", err)
	return q
}

func declararFanoutCola(ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(
		"",    // nombre de la cola
		false, // duradero
		false, // autoeliminable
		true,  // exclusivo
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
	failOnError("Failed to declare a queue", err)
	return q
}

func crearConsumir(ch *amqp.Channel, q amqp.Queue) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		q.Name, // nombre de la cola
		"",     // clave de enrutamiento
		true,   // autoeliminable
		false,  // exclusivo
		false,  // noWait
		false,  // noLocal
		nil,    // argumentos
	)
	failOnError("Failed to register a consumer", err)
	return msgs
}

func generarOperacio() int {
	num := 0
	for num == 0 {
		num = rand.Intn(21) - 10 // generates a random number between -5 and 5
	}
	return num
}

func failOnError(message string, err error) {
	if err != nil {
		fmt.Println(message, err)
	}
}
