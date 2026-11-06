<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class WorkerProdCommand extends Command
{
    protected $signature = 'kafka:worker-prod';
    protected $description = 'Worker da Produção: consome pedidos pagos e inicia fabrico';

    public function handle()
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));
        $conf->set('group.id', 'grupo-setor-producao');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(['pedidos-pagos']);

        $producerConf = new Conf();
        $producerConf->set('bootstrap.servers', config('services.kafka.brokers'));
        $producer = new Producer($producerConf);
        $topicProximo = $producer->newTopic('pedidos-em-producao');

        $this->info('[*] Worker de Produção ligado e à escuta...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[PROD] A fabricar pedido #{$dados['pedido_id']}...");

                $dados['status'] = 'Produzindo';
                $dados['producao_iniciada_em'] = now()->toDateTimeString();

                $payloadAtualizado = json_encode($dados);
                $topicProximo->produce(RD_KAFKA_PARTITION_UA, 0, $payloadAtualizado);
                $producer->flush(10000);

                $this->info("[PROD] Pedido #{$dados['pedido_id']} em fabrico! Status: [Produzindo] -> Enviado para a Expedição.");
            }
        }
    }
}