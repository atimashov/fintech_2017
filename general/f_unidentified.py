# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 01:21:11 2017

@author: Alex
"""
def values(variable):
    uni = variable.unique()
    total = 0
    for val in uni:
        print(val, ':', sum(variable == val))
        total = total + sum(variable == val)
    print('-----------')
    print('total:',total)

def binary(length):
    out = []
    for n_iter in range(2 ** length):
        a = [0] * length
        s = 0
        for i in range(length - 1):
            a[i] = (n_iter - s) / (2 ** (length - 1 - i))
            s=s + a[i] * (2 ** (length - 1 - i))
        a[length - 1] = n_iter % 2
        out.append(a)
    return out
    
def array_to(array):
    sup_binary = binary(len(array))    
    arrays = []
    for i in range(len(sup_binary)):
        a = []
        for j in range(len(sup_binary[i])):
           if sup_binary[i][j] == 1:
               a.append(array[j])
        arrays.append(a)
    return(arrays)
