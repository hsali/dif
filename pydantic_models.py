from datetime import date
from uuid import UUID, uuid4
from enum import Enum
from pydantic import BaseModel, EmailStr


class Department(Enum):
    HR = "HR"
    SALES = "SALES"
    IT = "IT"
    ENGINEERING = "ENGINEERING"


class Employee(BaseModel):
    employee_id: UUID = uuid4()
    name: str
    email: EmailStr
    date_of_birth: date
    salary: float
    department: Department
    elected_benefits: bool


empl = Employee(
    name="Chris DeTuma",
    email="cdetuma@example.com",
    date_of_birth="1998-04-02",
    salary=123_000.00,
    department="IT",
    elected_benefits=True,
)


empl1 = Employee(
    name="Chris DeTuma",
    email="cdetuma",
    date_of_birth="1998-04-02",
    salary=123_000.00,
    department="IT",
    elected_benefits=True,
)


new_employee_dict = {
    "name": "Chris DeTuma",
    "email": "cdetuma@example.com",
    "date_of_birth": "1998-04-02",
    "salary": 123_000.00,
    "department": "IT",
    "elected_benefits": True,
}
empl = Employee.model_validate(new_employee_dict)
new_employee_dict1 = {
    "name": "Chris DeTuma",
    "email": "cdetuma@example.com",
    "date_of_birth": "1998-04-02",
    "salary": 123_000.00,
    "department": "IT",
    "elected_benefits": True,
}

empl2 = Employee.model_validate(new_employee_dict1)


empl.model_dump()

empl.model_dump_json()

Employee.model_json_schema()

import pprint
pprint.pprint(Employee.model_json_schema())

import json 

pretty = json.dumps(empl.model_dump(), indent=4)
print(empl.model_dump_json(indent=4))