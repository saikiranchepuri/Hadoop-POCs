
use test;

create table test.BookCatalog(bookid string,author string,title string,genre string,price string,publish_date string,description string) ROW FORMAT delimited fields terminated by '|' lines terminated by '\n' stored as textfile;

LOAD DATA LOCAL INPATH '/home/hadoopuser/workspace_2/Hive_Jdbc_Project/input/hive_input.txt' INTO Table test.BookCatalog;

select * from test.bookcatalog;

select * from test.bookcatalog where author ='Corets, Eva';

select * from test.bookcatalog where genre='Fantasy';

select * from test.bookcatalog where price > 5;

select * from test.bookcatalog where publish_date > '2001-01-01';