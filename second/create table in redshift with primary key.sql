CREATE TABLE MYUSERS(
user_id INT PRIMARY KEY ,
user_first_name VARCHAR 30,
user_last_name VARCHAR 30,
);


select * from myusers;

INSERT INTO myusers(user_id,user_first_name,user_last_name)
values(1,"Scoot","Tigger");

UPDATE myusers
SET user_first_name='Mickey',user_last_name='Mouse'
WHERE user_id=2