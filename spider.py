import json
import urllib2
import sys, urllib
import csv
import string
import time


start = time.clock()
index=0
csvfile = open("queryD.csv")
reader = csv.reader(csvfile)
qlist = []
for line in reader:
	qlist.append(line[0])
	index+=1;


#print qlist
'''qlist = ["shoes"]

num = 1
for query,id,rank in csvfile :
	i =0 
	flag = True
	while(i < num ) :
		if query == qlist[i] :
			flag = False
			break
		else :
			i +=1
	if flag :
#		print query
		qlist.append(query)
		num +=1
#print "@@@@@@@@@@@@  FINISHED  @@@@@@@@@@@@@"
'''


#data_tiny.write("##")


j=6
d_t = open("tiny_data_B19.csv", "w")
while (j < 10) :

	print 'Iteration %d' % (j+1)
	url_template ="http://54.148.5.159:9200/snapdeal/product/_search?q=%s"
#	url = "http://54.148.5.159:9200/snapdeal/product/_search"
	def http_post(url):
	#	print url
		value = {'from':0+j*1000, 'size':999+j*1000}
		jvalue = json.dumps(value)
		#print url
		request = urllib2.Request(url,jvalue)
		response = urllib2.urlopen(request)
		return response.read()

	k =1628 
	num = 1649
	while (k<num) :
		print 'Iteration: %d' % (j+1)
		print 'Query: %d' %(k + 1)
		url = url_template % qlist[k]
		print 'url: %s' % url
		while True:
			try:
				content = http_post(url)
				break
			except Exception, e:
				time.sleep(0.1)
				print "error occurred!"
				print e

		data = json.loads(content)
		if data[u'hits'][u'total']-( 999+j*1000) < 999 :    ###
			min = data[u'hits'][u'total'] -(999+j*1000)
		else :
			min = 999    ###
		i = 0
		while (i < min ) :
			try:
				d_t.write('%s,%s,%s,%s'%(qlist[k], data[u'hits'][u'hits'][i][u'_id'], data[u'hits'][u'hits'][i][u'_score'], data[u'hits'][u'hits'][i][u'_source'][u'rank'])+'\r\n')
				i+=1
			except UnicodeDecodeError:
				print "UnicodeDecodeError!"
				break
		k += 1
	j+=1 

d_t.close()
end = time.clock()
print "time-cost :", end-start
