# Annotweet
## Overview
It's a **Sentiment Analysis** project on tweets. 

Project structure (leveraging simple Builder and Factory patterns) aims at making easy the creation of predictive models out of a fully configurable **Spark ML pipeline** (choose pre-processings, transformations, and classification algorithm).

We have also convenient class for **tweet extracting** through *twitter4j* facilities
We made our own annotation on an extracted dataset.

## With
- Scala 2.11.8
- Spark 2.4.0
- Java 8

See detail on how to use trained model in `Guide_Utilisation.pdf`in `Compte_Rendu.pdf` (in French).

# Rendering: Sentiment Analysis Approaches to tweets about EM

## Extraction
Our code used for extraction is based on the twitter4j API and is contained in the package of our project `com.enzobnl.annotweet.extraction`
## Annotation
We annotated the tweets each one on our side and we made an automatic concensus based on rules.

## Techno used
Spark ML in Scala

## Tokenization
This is the part on which we have invested the most time.
Here is a preview of the steps implemented from an example:

|Original|“Macron il a une bonne testas,on dirait un Makroudh! Mais lui,il a #rien de bon:( #aie https://t.co/FiOiho7”|
|--|--|
| smileys | “Macron il a une bonne testas,on dirait un Makroudh! Mais lui,il a #rien de bon **sadsmiley** #aie https://t.co/FiOiho7” |
|URLs|“Macron il a une bonne testas,on dirait un Makroudh! Mais lui,il a #rien de bon sadsmiley  #aie **http FiOiho7**“|
|Ponctuation|“Macron il a une bonne testas  on dirait un Makroudh **!**  Mais lui  il a #rien de bon sadsmiley  #aie http FiOiho7”|
|Split|[“Macron”, “il”, “a”, “une”, “bonne”, “testas”, “on”, “dirait”, “un”, “Makroudh”, “!”, “Mais”, “lui”, “il”, “a”, “#rien”, “de”, “bon”, “sadsmiley”, “#aie”, “http”, “FiOiho7”]|
|Filtre “Mais”|[“lui”, “il”, “a”, “#rien”, “de”, “bon”, “sadsmiley”, “#aie”, “http”, “FiOiho7”]|
|Fillers|[“lui”, “il”, “a”, “#rien”, “bon”, “sadsmiley”, “#aie”, “http”, “FiOiho7”]|
|Hashtags|[“lui”, “il”, “a”, **“rien”**, “bon”, “sadsmiley”, “#aie”, “http”, “FiOiho7”, **“#rien”**]|
|Paires de mots|[“luiil”, “ila”, “arien”, “rienbon”, “bonsadsmiley”, “lui”, “il”, “a”, “rien”, “bon”, “sadsmiley”, “#aie”, “http”, “FiOiho7”, “#rien”]|

Word pairs are especially effective when the rest of the system (vectorization and / or classification) do not allow to take into account the context of the words, for example TF-IDF + Logistic Regression.

Each step was selected because it provided an improvement in accuracy, however we could spend more time trying to understand the influence of each step on the results to refine them: What are the errors they remove? what are the errors they add?

## Vectorization
For vectorization, we were initially on a ** TF-IDF **, then we drastically increased our performance by 7 points by passing on ** word2vec **.

## Classification
For the choice of the classification algorithm, we tested (ordered from the best to the least good):
1. Gradient Boosting Tree (best)
2. Logistic Regression
3. Khiops (Orange proprietary implementation of the MODL approach)
4. Random Forest

We could not tune our GBT as much as we would have liked (only the number of iteration and the learning rate were optimized).

## Model Selection Methodology
Whether for tokenization modifications, vectorization solutions or classification, we evaluated our global models at each step using cross-validations (number of blocks = 10).

## Results
On our cross-validation we get an accuracy of ** 68% + - 2% ** with the previously presented tokenization + Word2Vec + GBT.
Special mention to the time spent on tokenization which allowed us to gain between 2 and 7 points of accuracy, according to the vectorization and the classification that am, the most important improvement being observed when we follow with TF-IDF + Regression Logistics.


