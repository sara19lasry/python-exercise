lines_of_text = []
n = 10000

for i in range(1, n+1):
    lines_of_text.append("Hi, this is the "+str(i)+"st line\n")

with open('data/test.txt','w') as fh:
	fh.writelines(lines_of_text) 
	fh.close() 