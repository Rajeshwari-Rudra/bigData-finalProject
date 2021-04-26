# bigData-finalProject
This project is based on displaying the word count in a file using PySpark and Databricks in cloud.


## Databricks published link
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3499787024332230/3001626945718810/490161810778949/latest.html

## Key Words
- Resilient Distributed Dataset (RDD) - A distributed collection of Objects
- DataFrame -An immutable distributed collection of data.
- SparkContext(sc) - entry point to any spark functionality.
- Collect()- It is an action operation that is used to retrieve all the elements of the dataset (from all nodes) to the driver node.

## Input Source
- The input can be taken as any URL which is of the text format.For this project, I have taken data as [The Project Gutenberg EBook of My Life and Work, by Henry Ford](https://www.gutenberg.org/cache/epub/7213/pg7213.txt)

## Commands to Start this project
------------------------------------------------------------------------
## Step 1:-  Data Injection
1. Initially, import all the required libraries and start fetching the data from the URL 
```python
import urllib.request 
stringInURL = "https://www.gutenberg.org/cache/epub/7213/pg7213.txt"
urllib.request.urlretrieve(stringInURL,"/tmp/lifeandwork.txt")
```
2. Next, move the file from temp folder to databricks storage folder of dbfs

```python
dbutils.fs.mv("file:/tmp/lifeandwork.txt","dbfs:/data/lifeandwork.txt")
```
3. Then, transfer the data file into Spark using sparkContext 
```python
lifeandworkRawRDD= sc.textFile("dbfs:/data/lifeandwork.txt")
```
## Step 2:- Cleaning the data
1. Command to separate the words from each line using flatmap function and to change all the words to lower case and then removing the spaces between them
```python
lifeandworkMessyTokensRDD = lifeandworkRawRDD.flatMap(lambda eachLine: eachLine.lower().strip().split(" "))
```
2. After changing the words to lowercase, we need to remove punctuations using regular expression by importing "Regex" library
```python
import re
wordsAfterCleanedTokensRDD = lifeandworkMessyTokensRDD.map(lambda letter: re.sub(r'[^A-Za-z]', '', letter))
```
3. Then, we need to filter the data by removing all the stop words from the data and create a new RDD with the obtained result
```python
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover()
stopwords = remover.getStopWords()
lifeAndWorkWordsRDD = wordsAfterCleanedTokensRDD.filter(lambda word: word not in stopwords)
# removing all the empty spaces from the data
lifeAndWorkRemoveSpaceRDD = lifeAndWorkWordsRDD.filter(lambda x: x != "")
```
## Step 3:- Processing the data
* In this process, we will pair up each word of the file and count it to 1 as an intermediate Key-value pairs and then we need to transform the words using reduceByKey() to get the total count of all the distinct words. To get back to python, we use collect() and then print the obtained results.
```python
lifeAndWorkPairsRDD = lifeAndWorkRemoveSpaceRDD.map(lambda eachWord: (eachWord,1))
# transforming the words using reduceByKey() to get (word,count) results
lifeAndWorkWordCountRDD = lifeAndWorkPairsRDD.reduceByKey(lambda acc, value: acc + value)
#collect() action to get back to python
results = lifeAndWorkWordCountRDD.collect()
print(results)
```
* Sorting the words based on highest count of the word and displaying them in descending order
```python
output = sorted(results, key=lambda t: t[1], reverse=True)[:15]
print(output)
```
## Charting the data
* To display the data by ploting the obtained output, here we need to import the required libraries and then label the axis as per our requirement.  
```python
import pandas as pd  
import matplotlib.pyplot as plt
import seaborn as sns

# preparing chart information
source = 'The Project Gutenberg EBook of My Life and Work, by Henry Ford'
title = 'Top Words in ' + source
xlabel = 'Words'
ylabel = 'Count'

df = pd.DataFrame.from_records(output, columns =[xlabel, ylabel]) 
plt.figure(figsize=(20,4))
sns.barplot(xlabel, ylabel, data=df, palette="cubehelix").set_title(title)
```
## Result 
![Output after processing the data](https://github.com/Rajeshwari-Rudra/bigData-finalProject/blob/main/output1.png?raw=true)
![Output after Charting the data](https://github.com/Rajeshwari-Rudra/bigData-finalProject/blob/main/barGraph.png?raw=true)

## To create an image of word cloud
* We need to import "Natural Language tool kit" and "word Cloud" libraries to show the highest word count for the given input file.Then, we need to define few functions to process the data and the result will be shown in figure.
```python
import matplotlib.pyplot as plt
import nltk
import wordcloud
from nltk.corpus import stopwords # to remove the stopwords from the data
from nltk.tokenize import word_tokenize #  breaking down the text into smaller units called tokens
from wordcloud import WordCloud # creating an image using words in the data

# defining the functions to process the data
class WordCloudGeneration:
    def preprocessing(self, data):
        # convert all words to lowercase
        data = [item.lower() for item in data]
        # load the stop_words of english
        stop_words = set(stopwords.words('english'))
        # concatenate all the data with spaces.
        paragraph = ' '.join(data)
        # tokenize the paragraph using the inbuilt tokenizer
        word_tokens = word_tokenize(paragraph) 
        # filter words present in stopwords list 
        preprocessed_data = ' '.join([word for word in word_tokens if not word in stop_words])
        print("\n Preprocessed Data: " ,preprocessed_data)
        return preprocessed_data

    def create_word_cloud(self, final_data):
        # initiate WordCloud object with parameters width, height, maximum font size and background color
        # call the generate method of WordCloud class to generate an image
        wordcloud = WordCloud(width=1600, height=800, max_font_size=200, background_color="white").generate(final_data)
        # plt the image generated by WordCloud class
        plt.figure(figsize=(12,10))
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.show()

wordcloud_generator = WordCloudGeneration()
# you may uncomment the following line to use custom input
# input_text = input("Enter the text here: ")
import urllib.request
url = "https://www.gutenberg.org/cache/epub/7213/pg7213.txt"
request = urllib.request.Request(url)
response = urllib.request.urlopen(request)
input_text = response.read().decode('utf-8')

input_text1 = input_text.split('.')
clean_data = wordcloud_generator.preprocessing(input_text1)
wordcloud_generator.create_word_cloud(clean_data)

```
## Result of word Cloud
![](https://github.com/Rajeshwari-Rudra/bigData-finalProject/blob/main/wordCloud.png?raw=true)

## Conclusion
Based on the obtained results, the top 15 words of 'The Project Gutenberg EBook of My Life and Work, by Henry Ford' are 'one','business','work', 'man', 'men','money', 'time','made','make','may','car','every','production','get' and 'much'.We can say that the author is mainly focussing on individuals who spend their most of the time in business or at work to make more productive by making money.

##  References
- [Introduction to PySpark](https://github.com/denisecase/starting-spark)
- [To get the ouput in descending order](https://stackoverflow.com/questions/41306684/get-top-5-largest-from-list-of-tuples-python/41306701)
- [Spark Key Terms](https://sparkbyexamples.com/)
- [To build an image using  WordCloud](https://www.section.io/engineering-education/word-cloud/)

