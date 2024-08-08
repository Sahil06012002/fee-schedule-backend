from enum import Enum

class Operations(Enum):
    UPDATE = 'UPDATE'
    ADD = 'ADD'
    DELETE = 'DELETE'

class Axis(Enum) : 
    ROW = 'ROW'
    COLUMN = 'COLUMN'