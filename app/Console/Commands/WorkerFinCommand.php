<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class WorkerFinCommand extends Command
{
    protected $signature = 'kafka:worker-fin';
    protected $description = 'Worker do Financeiro: consome pedidos OK e aprova para Pago';

    public function handle()
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));
        $conf->set('group.id', 'grupo-setor-financeiro');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(['pedidos-aprovados-adm']);

        $producerConf = new Conf();
        $producerConf->set('bootstrap.servers', config('services.kafka.brokers'));
        $producer = new Producer($producerConf);
        $topicProximo = $producer->newTopic('pedidos-pagos');

        $this->info('[*] Worker Financeiro ligado e à escuta...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[FIN] A validar pagamento do pedido #{$dados['pedido_id']}...");

                $dados['status'] = 'Pago';
                $dados['financeiro_processado_em'] = now()->toDateTimeString();

                $payloadAtualizado = json_encode($dados);
                $topicProximo->produce(RD_KAFKA_PARTITION_UA, 0, $payloadAtualizado);
                $producer->flush(10000);

                $this->info("[FIN] Pedido #{$dados['pedido_id']} pago! Status: [Pago] -> Enviado para a Produção.");
            }
        }
    }
}