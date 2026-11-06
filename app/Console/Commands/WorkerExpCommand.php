<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class WorkerExpCommand extends Command
{
    protected $signature = 'kafka:worker-exp';
    protected $description = 'Worker de Expedição: finaliza o ciclo do pedido';

    public function handle()
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));
        $conf->set('group.id', 'grupo-setor-expedicao');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(['pedidos-em-producao']);

        $this->info('[*] Worker de Expedição ligado e à escuta...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[EXP] A embalar e despachar pedido #{$dados['pedido_id']}...");

                $dados['status'] = 'Finalizado';
                $dados['finalizado_em'] = now()->toDateTimeString();

                $this->info("[EXP] SUCESSO! Pedido #{$dados['pedido_id']} concluído com status: [Finalizado]. Fim da linha.");
            }
        }
    }
}