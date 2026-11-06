<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use RdKafka\Conf;
use RdKafka\Producer;

class CheckoutController extends Controller
{
    public function store(Request $request)
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));

        $producer = new Producer($conf);
        $topic = $producer->newTopic('pedidos-criados');

        $payload = json_encode([
            'pedido_id' => rand(1000, 9999),
            'cliente' => $request->input('cliente', 'Cliente Padrão'),
            'status' => 'Novo',
            'criado_em' => now()->toDateTimeString()
        ]);

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload);
        $producer->flush(10000);

        return response()->json([
            'mensagem' => 'Pedido fechado com sucesso e enviado para o Kafka!',
            'dados' => json_decode($payload)
        ], 201);
    }
}