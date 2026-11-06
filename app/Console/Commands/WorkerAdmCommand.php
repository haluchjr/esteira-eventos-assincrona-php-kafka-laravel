<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class WorkerAdmCommand extends Command
{
    protected $signature = 'kafka:worker-adm';
    protected $description = 'Worker do Setor Administrativo: consome pedidos criados e aprova para OK';

    public function handle()
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));
        $conf->set('group.id', 'grupo-setor-administrativo');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(['pedidos-criados']);

        // Produtor interno para enviar para a próxima etapa
        $producerConf = new Conf();
        $producerConf->set('bootstrap.servers', config('services.kafka.brokers'));
        $producer = new Producer($producerConf);
        $topicProximo = $producer->newTopic('pedidos-aprovados-adm');

        $this->info('[*] Worker Administrativo ligado e à escuta em ' . config('services.kafka.brokers') . '...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[ADM] A processar pedido #{$dados['pedido_id']}...");

                // Atualiza o status
                $dados['status'] = 'OK';
                $dados['adm_processado_em'] = now()->toDateTimeString();

                $payloadAtualizado = json_encode($dados);
                $topicProximo->produce(RD_KAFKA_PARTITION_UA, 0, $payloadAtualizado);
                $producer->flush(10000);

                $this->info("[ADM] Pedido #{$dados['pedido_id']} aprovado! Status: [OK] -> Enviado para o Financeiro.");
            }
        }
    }
}