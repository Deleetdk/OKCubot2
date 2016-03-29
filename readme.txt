1. Run the project
In OKcu/Okcu directory, run the following command
$  python start.py user_email user_password

2. Result
After finishing the program, you can see the new directory(like 'data2016-03-27 12:55:04') in OKcu/Okcu/.
In this directory, there are data files according to each person.

3. How to change the number of people to scrape.

In line 11 of OKcu/Okcu/spider/okcubot_spider.py, you can change the variable 'max_people'.

4. Working environment

- python 2.7
- python scrapy1.0.5
- ubuntu 14.04

5. New things

- I use your code for saving data in csv file.
- I scraped profile text and images.
You can see images in data/pictures/
- I scraped the questions in the first question page, not based on the categories.

