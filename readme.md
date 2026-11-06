# Esteira de Eventos Assíncrona com Laravel 12 e Kafka

Esta documentação descreve a implementação da arquitetura orientada a eventos (EDA) utilizando o **Laravel 12** integrado ao **Apache Kafka** via extensão `rdkafka`.

Nesta abordagem, os setores da empresa funcionam como **daemons (workers em segundo plano)** executados via Comandos Artisan nativos (`php artisan kafka:worker-*`). A esteira pode rodar tanto de forma 100% automatizada quanto com pausas para validações manuais ou externas.

---

## 🛠️ Arquitetura do Fluxo

```text
[ Cliente (HTTP POST) ] 
       │
       ▼
[ CheckoutController ] ────────► (Tópico: pedidos-criados) [Status: Novo]
                                         │
                                         ▼
                             [ WorkerAdmCommand ] ────► (Tópico: pedidos-aprovados-adm) [Status: OK]
                                                                  │
                                                                  ▼
                                                      [ WorkerFinCommand ] ───► (Tópico: pedidos-pagos) [Status: Pago]
                                                                                       │
                                                                                       ▼
                                                                           [ WorkerProdCommand ] ───► (Tópico: pedidos-em-producao) [Status: Produzindo]
                                                                                                            │
                                                                                                            ▼
                                                                                                [ WorkerExpCommand ] ───► [Status: Finalizado]
```

---

## 📋 Passo a Passo de Configuração

### 1. Configuração de Serviços e Variáveis de Ambiente

No arquivo `config/services.php`, adicione as credenciais do Kafka:

```php
'kafka' => [
    'brokers' => env('KAFKA_BROKERS', 'kafka:29092'),
],
```

No arquivo `.env` da raiz do Laravel:

```env
KAFKA_BROKERS=kafka:29092
```

---

### 2. Ativação de Rotas de API (Laravel 12)

Execute o comando no container para publicar o arquivo de rotas de API:

```bash
docker compose exec backend php artisan install:api
```

Em `routes/api.php`, registre a rota de checkout:

```php
use App\Http\Controllers\CheckoutController;
use Illuminate\Support\Facades\Route;

Route::post('/checkout', [CheckoutController::class, 'store']);
```

---

### 3. Controller de Checkout (`app/Http/Controllers/CheckoutController.php`)

Dispara o evento inicial (`pedidos-criados`) assim que uma requisição HTTP de compra é recebida:

```php
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
```

---

### 4. Workers da Esteira (Console Commands)

Crie os comandos Artisan para cada setor executando:

```bash
docker compose exec backend php artisan make:command WorkerAdmCommand
docker compose exec backend php artisan make:command WorkerFinCommand
docker compose exec backend php artisan make:command WorkerProdCommand
docker compose exec backend php artisan make:command WorkerExpCommand
```

#### Setor Administrativo (`app/Console/Commands/WorkerAdmCommand.php`)

```php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class WorkerAdmCommand extends Command
{
    protected $signature = 'kafka:worker-adm';
    protected $description = 'Worker Administrativo: valida pedidos criados e aprova para OK';

    public function handle()
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));
        $conf->set('group.id', 'grupo-setor-administrativo');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(['pedidos-criados']);

        $producerConf = new Conf();
        $producerConf->set('bootstrap.servers', config('services.kafka.brokers'));
        $producer = new Producer($producerConf);
        $topicProximo = $producer->newTopic('pedidos-aprovados-adm');

        $this->info('[*] Worker Administrativo ligado...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[ADM] Processando pedido #{$dados['pedido_id']}...");

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
```

#### Setor Financeiro (`app/Console/Commands/WorkerFinCommand.php`)

```php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class WorkerFinCommand extends Command
{
    protected $signature = 'kafka:worker-fin';
    protected $description = 'Worker Financeiro: valida pagamento e aprova para Pago';

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

        $this->info('[*] Worker Financeiro ligado...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[FIN] Validando pagamento do pedido #{$dados['pedido_id']}...");

                $dados['status'] = 'Pago';
                $dados['financeiro_processado_em'] = now()->toDateTimeString();

                $payloadAtualizado = json_encode($dados);
                $topicProximo->produce(RD_KAFKA_PARTITION_UA, 0, $payloadAtualizado);
                $producer->flush(10000);

                $this->info("[FIN] Pedido #{$dados['pedido_id']} pago! Status: [Pago] -> Enviado para Produção.");
            }
        }
    }
}
```

#### Setor de Produção (`app/Console/Commands/WorkerProdCommand.php`)

```php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

class WorkerProdCommand extends Command
{
    protected $signature = 'kafka:worker-prod';
    protected $description = 'Worker Produção: inicia fabricação do pedido';

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

        $this->info('[*] Worker de Produção ligado...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[PROD] Fabricando pedido #{$dados['pedido_id']}...");

                $dados['status'] = 'Produzindo';
                $dados['producao_iniciada_em'] = now()->toDateTimeString();

                $payloadAtualizado = json_encode($dados);
                $topicProximo->produce(RD_KAFKA_PARTITION_UA, 0, $payloadAtualizado);
                $producer->flush(10000);

                $this->info("[PROD] Pedido #{$dados['pedido_id']} em fabricação! Status: [Produzindo] -> Enviado para Expedição.");
            }
        }
    }
}
```

#### Setor de Expedição (`app/Console/Commands/WorkerExpCommand.php`)

```php
namespace App\Console\Commands;

use Illuminate\Console\Command;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class WorkerExpCommand extends Command
{
    protected $signature = 'kafka:worker-exp';
    protected $description = 'Worker Expedição: finaliza e despacha o pedido';

    public function handle()
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', config('services.kafka.brokers'));
        $conf->set('group.id', 'grupo-setor-expedicao');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe(['pedidos-em-producao']);

        $this->info('[*] Worker de Expedição ligado...');

        while (true) {
            $message = $consumer->consume(1000);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $dados = json_decode($message->payload, true);

                $this->line("[EXP] Embalando e despachando pedido #{$dados['pedido_id']}...");

                $dados['status'] = 'Finalizado';
                $dados['finalizado_em'] = now()->toDateTimeString();

                $this->info("[EXP] SUCESSO! Pedido #{$dados['pedido_id']} finalizado com sucesso.");
            }
        }
    }
}
```

---

## 🕹️ Guia de Comandos Docker e Execução

### 1. Iniciar o Ambiente Containerizado

```bash
docker compose up -d --build
```

### 2. Iniciar os Workers em Background

Abra 4 abas/terminais apontando para o container `backend` e execute os comandos de trás para a frente:

```bash
# Terminal 1 - Expedição
docker compose exec backend php artisan kafka:worker-exp

# Terminal 2 - Produção
docker compose exec backend php artisan kafka:worker-prod

# Terminal 3 - Financeiro
docker compose exec backend php artisan kafka:worker-fin

# Terminal 4 - Administrativo
docker compose exec backend php artisan kafka:worker-adm
```

### 3. Disparar uma Requisição de Teste (Gatilho HTTP)

Num 5º terminal, faça a chamada para a API de checkout do Laravel:

```bash
curl -X POST http://localhost:8088/api/checkout \
  -H "Content-Type: application/json" \
  -d '{"cliente": "Empresa XPTO Laravel 12"}'
```

---

## 💡 Práticas para Produção

1. **Gestão de Processos (Supervisor):** Em ambiente produtivo, configure o `supervisor` dentro do container para manter os workers rodando como daemons e reiniciá-los automaticamente em caso de exceções imprevistas.
2. **Escalabilidade Horizontal:** É possível rodar múltiplas instâncias do mesmo comando `kafka:worker-*` simultaneamente. O Kafka dividirá as partições automaticamente entre os membros do mesmo `group.id`.
3. **Persistência com Eloquent:** Dentro do bloco de processamento de cada worker, é recomendado utilizar `DB::transaction()` para atualizar os modelos do Eloquent antes de publicar o próximo evento.