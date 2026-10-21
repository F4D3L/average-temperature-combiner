# Temperatura Média com Combiner — MapReduce

Atividade de Big Data (PUCPR): a atividade de temperatura média do arquivo `forestfireinput.csv`
refeita com a otimização da Aula 06, o **Combiner**.

## Como funciona

O arquivo não tem cabeçalho, e a temperatura está na coluna 8 (contando a partir de 0). O Map
emite cada leitura com a mesma chave global e o valor composto `(temp, 1)`. O Combiner roda logo
depois de cada Map e junta tudo o que aquele Map produziu em uma única soma parcial
`(soma, quantidade)`. O Reducer soma as somas parciais e divide uma única vez, no final.

```
Map:      "7,5,mar,fri,...,8.2,..."     ->  ("temperatura", (8.2, 1))
Combiner: soma a saída de cada Map      ->  ("temperatura", (3189.2, 177))
Shuffle:  agrupa pela chave             ->  ("temperatura", [(3189.2, 177), (3097.0, 169), ...])
Reduce:   soma(somas) / soma(quant.)    ->  ("temperatura", 18.889168278529986)
```

O Combiner **não divide**. Se emitisse a média parcial de cada Map, o Reducer tiraria a média das
médias, o erro mostrado na Aula 05. Com o CSV dividido em 3 partes, as médias parciais são 18,0181,
18,3254 e 20,3480, e a média delas dá 18,8972, e não 18,8892. Como o Combiner soma e quem divide
é só o Reducer, o resultado é o mesmo com ou sem Combiner, e não importa quantas vezes o Hadoop
decida executá-lo.

| Classe | Papel |
| --- | --- |
| `AverageTemperature` | Driver (`main`): configura o job e registra o Combiner |
| `TemperatureMapper` | Map: lê uma linha e emite `("temperatura", (temp, 1))` |
| `TemperatureCombiner` | Combiner: soma a saída de cada Map em `("temperatura", (soma, quantidade))` |
| `TemperatureReducer` | Reduce: soma as somas parciais e as quantidades e emite a média |
| `SumCountWritable` | `Writable` customizado para o valor composto `(soma, quantidade)` |

## O ganho

Contadores do Hadoop para o arquivo inteiro (517 linhas, um único Map):

| Contador | Sem Combiner | Com Combiner |
| --- | --- | --- |
| Map output records | 517 | 517 |
| Combine output records | — | 1 |
| Reduce input records | 517 | **1** |

Com o CSV dividido em 3 arquivos (3 Maps, como os 3 nós do slide 06), o Reducer recebe 3 registros
em vez de 517, e o resultado continua sendo a média exata.

Resultado: **18,889168278529986 °C** sobre 517 leituras (soma 9765,7).

## Como executar

Requisitos: Maven e **Java 8, 11 ou 17**. O Hadoop 3.3.6 não roda no Java 23 ou superior.

```bash
mvn package                                   # gera target/average-temperature-combiner-1.0.0.jar

# Em uma máquina/VM com Hadoop instalado (o diretório de saída não pode existir):
hdfs dfs -put input/forestfireinput.csv /forestfire/input.csv
hadoop jar target/average-temperature-combiner-1.0.0.jar /forestfire/input.csv /forestfire/output-combiner
hdfs dfs -cat /forestfire/output-combiner/part-r-00000
```

No Windows, com Docker (no PowerShell, dentro da pasta do projeto):

```powershell
docker run --rm -v "${PWD}:/app" -w /app maven:3.9-eclipse-temurin-11 mvn -q clean package exec:java
Get-Content target\output\part-r-00000
```

Os contadores `Combine input records`, `Combine output records` e `Reduce input records` aparecem
no console ao final do job.
