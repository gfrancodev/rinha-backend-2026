# Rinha Backend 2026

Implementação em Bun (TypeScript) para a Rinha de Backend 2026, com detecção de
fraude por busca vetorial em árvore VP (Vantage Point).

## Estratégia

- O build da imagem descarrega o `references.json.gz` oficial e gera um
  `references.bin` com vetores `float32` e rótulos, embutido em
  `/opt/references.bin`.
- Cada API carrega `mcc_risk.json` e `normalization.json` a partir de
  `DATA_PATH`, monta o vetor de 14 dimensões conforme a especificação e faz
  k-NN com **k = 5** e distância euclidiana sobre o índice VP em memória.
- `fraud_score` é a fração de fraudes entre os 5 vizinhos; `approved` segue o
  limiar fixo **0.6**.
- O load balancer é Nginx em round-robin sobre Unix sockets e não inspeciona o
  payload.

## Rodar

```sh
docker compose up --build
curl http://localhost:9999/ready
```

Para o teste oficial/local da Rinha, usa o `run.sh` do repositório oficial com a
stack a escutar em `localhost:9999`.

## Regras

- `GET /ready`
- `POST /fraud-score`
- Porta `9999`
- 1 load balancer + 2 instâncias da API
- `docker-compose.yml` na raiz
- Limite declarado: `1.0 CPU` e `350 MB`
- Sem lookup dos payloads de teste