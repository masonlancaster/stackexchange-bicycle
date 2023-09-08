create schema stackoverflow;

create table bicycle_questions (
title varchar(250), 
body_markdown varchar(1000),
link varchar(250),
question_id int,
creation_date varchar(100),
view_count int,
answer_count int,
is_answer boolean);


