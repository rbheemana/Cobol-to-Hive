# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 14:37:02 2016

@author: Manu
"""
import pickle
#import json
global memory
import imp
import os
#memory =dict({})

def save_memory(a):
    with open('memory.pickle', 'wb') as handle:
#    with open('memory.json','a') as handle:
        pickle.dump(a, handle)
#        json.dump(a,handle)

def load_memory():
    try:
#        with open('memory.json') as handle:
#            return json.load(handle)
        with open('memory.pickle', 'rb') as handle:
            return pickle.load(handle)
    except FileNotFoundError:
        return dict({})

def expand_vocabulary(words):
    for word in words:
        new_entity = word.lower() 
#       print(new_entity)
#       print(memory)
#       if memory[new_entity] == None:
        template = 'class {new_entity}:\n\tdef __init__(self):\n\t\tself.shape = "round"'
        context = {
        "new_entity":new_entity
        }
        file_name=new_entity+'.py'
        file=open(file_name,'w')
        file.write(template.format(**context))
        file.close()
#        new_class = map(__import__, new_entity)
#        a = exec(new_entity)()
        
        b=load_from_file(new_entity+'.py',new_entity)
            #print(b.edible())
#        print(a.edible())
#        new_entity_obj = type(new_entity,(),{})
        print(b.shape)
        memory[new_entity] = b
 


def load_from_file(filepath,expected_class):
    class_inst = None
    #expected_class = 'MyClass'

    mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])

    if file_ext.lower() == '.py':
        py_mod = imp.load_source(mod_name, filepath)

    elif file_ext.lower() == '.pyc':
        py_mod = imp.load_compiled(mod_name, filepath)

    if hasattr(py_mod, expected_class):
        class_inst = getattr(py_mod, expected_class)()

    return class_inst   
listen = ""
print("Hi..")
memory = load_memory()
#print(memory)
while listen != "Bye":
    listen = input()
    words = listen.split()
    expand_vocabulary(words)
print(memory)
save_memory(memory)
    
