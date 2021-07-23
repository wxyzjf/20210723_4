x = ['a','a','b','b','v','a','a','a','c','c','a','a','a']

a = 0
while a < len(x)-1:
  print ('a:',a,'len:',len(x)-1)
  if x[a] == x[a+1]:
     print('remove:',x[a+1])
     del x[a+1]
     print('x[0]:',x[0])
  else:
    a += 1
print(x)
