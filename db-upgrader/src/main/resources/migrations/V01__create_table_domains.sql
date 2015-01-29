CREATE TABLE domains (
  id INT NOT NULL AUTO_INCREMENT,
  title VARCHAR(50),
  description VARCHAR(200)
);

ALTER TABLE domains ADD PRIMARY KEY (id);

