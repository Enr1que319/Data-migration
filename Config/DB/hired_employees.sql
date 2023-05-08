CREATE TABLE hired_employees (
    id int NOT NULL,
    name varchar(50) NOT NULL,
    datetime varchar(20) NOT NULL,
    department_id int NOT NULL,
    job_id int NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);