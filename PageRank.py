#######################################
######### PageRank Program ############
# Version: 0.1
#
# Author: Chet Mancini
# Email: cam479 at cornell dot edu
# Web: chetmancini.com
# Warranty: None at all
#
# Dependencies: 
# 	- BeautifulSoup 
#		(http://www.crummy.com/software/BeautifulSoup/)
#
# Credits
#	- Web crawler code inspired by James Mills.
#		(http://code.activestate.com/recipes/576551-simple-web-crawler/)
#
# CS4300, Fall 2011
# Cornell University
# Assignment 3
#
# Where urls.txt is a text file of urls, one per line.
# Usage: 
# 	To refetch from web:
# 	$ python PageRank.py -crawl urls.txt [-v]
# 
# 	To use precomputed pagerank values:
#	$ python PageRank.py [-v]
#
#######################################

############   Imports  ###############
from __future__ import division
from BeautifulSoup import BeautifulSoup
from cgi import escape
import sys
import os
import math
import httplib
import urllib2
import urlparse
import string
import operator 
import codecs
import copy
import re

########## Basic Information #########
__version__ = "0.1"
__copyright__ = "copyright (c) 2011 Chet Mancini"
__author__ = "Chet Mancini"
__author_email__ = "cam479 at cornell dot edu"

USAGE = "%prog [options]"
VERSION = "%prog v" + __version__
AGENT = "%s/%s" % (__name__, __version__)

############ Globals #################
STOPWORDS = set(['and', 'is', 'it', 'an', 'as', 'at', 'have', 'in', 'its', 'are', 
	'said', 'from', 'for', 'to', 'been', 'than', 'also', 'other', 'which', 'new', 
	'has', 'was', 'more', 'be', 'we', 'that', 'of', 'but', 'they', 'not', 'with', 
	'by', 'a', 'on', 'this', '2011', 'could', 'their', 'these', 'can', 'the', 'or', 'first'])

DEBUG_FLG = True
VERBOSE_FLG = False
alpha = 0.15
CONVERGE_ERROR = 0.00001
############# Classes ################
'''
Unique ID spooler
'''
class UidSpooler:
	'''
	constructor
	'''
	def __init__(self, start=0):
		self.nextid = start
	
	'''
	Get the next uid
	'''
	def getNext(self):
		toReturn = self.nextid
		self.nextid += 1
		return toReturn

	'''
	Get the largest returned
	'''
	def getLargest(self):
		return self.nextid

'''
Page

Represent a fetched page from the web with all important information.
'''
class Page:

	'''
	Constructor
	'''
	def __init__(self, uid, url, title=""):
		# Unique id of this page (used as index to matrix)
		self.uid = uid

		# This pages' url
		self.url = url

		# This pages' title
		self.title = title

		# This pages' content
		self.content = ""

		# pages this links to
		self.linkUrls = set([])

		# (url, anchor) pairs this links to
		self.links = []

		# PageRank of this page
		self.pageRank = 0.0

		# Terms of anchors pointing to this page
		self.incomingTerms = []

	'''
	Get item
	'''
	def __getitem__(self, x):
		return self.urls[x]

	'''
	Add a link, which is a url and anchor
	'''
	def addLink(self, url, anchor):
		if url not in self.linkUrls:
			self.links.append((url, anchor))
			self.linkUrls.add(url)

	'''
	Add incoming terms from foreign page anchors. Lowercase and remove stopwords.
	'''
	def addIncomingTerms(self, termList):
		termList = map(lambda x: x.lower(), termList)
		termList = filter(lambda x: (x not in STOPWORDS), termList)
		self.incomingTerms.extend(termList)

	'''
	Is this page a deadend page?
	'''
	def isDeadend(self):
		if len(self.links) == 0:
			return True
		elif (len(self.links) == 1) and (self.links[0][0] == self.url):
			return True
		else:
			return False

	'''
	Get the title (defaults to none)
	'''
	def getTitle(self):
		if not self.title:
			return '<No Title>'
		else:
			return self.title

	'''
	Create a small index record from this page
	'''
	def getIndexRecord(self):
		return PageIndexRecord(self.title, self.incomingTerms)

	'''
	Save page data
	'''
	def savePage(self):
		filename = "pages/" + str(self.uid) + '.html'
		with open(filename, 'w') as f:
			f.write(self.content)
			if DEBUG_FLG:
				print 'page', self.uid, 'saved as', filename
		

'''
Web Crawler

Crawls a given set of pages and returns page data.
'''
class WebCrawler:
	'''
	Constructor
	'''
	def __init__(self, seedUrls=[]):
		self.pages = {}
		self.urls = set(seedUrls)
		self.spooler = UidSpooler()

	'''
	Returns whether a url is within scope of this program (excluding /person and /node and external pages)
	'''
	def validateUrl(self, url):
		return url in self.urls

	'''
	This url is a standard link that we're looking for.
	'''
	def standardLink(self, href):
		test = href
		if test[:31] != "http://www.library.cornell.edu/":
			return False
		elif string.find(href, "/node/") > -1:
			return False
		elif string.find(href, "/person/") > -1:
			return False
		else:
			return True

	'''
	santize a url
	'''
	def sanitizeUrl(self, url):
		url = str(url)
		if url[-10:] == "index.html":
			return url[:-10]
		
		hashIndex = string.find(url, "#")
		if hashIndex > -1:
			realUrl = url[0:hashIndex]
			return realUrl
		return url

	'''
	Open a request to a URL
	'''
	def openRequest(self, url):
		try:
			request = urllib2.Request(url)
			handle = urllib2.build_opener()
		except IOError:
			return None
		return (request, handle)

	def addHeaders(self, request):
		request.add_header("User-Agent", AGENT)

	'''
	Return a page object to a given url
	'''
	def fetchPage(self, url):
		if url in self.pages:
			return self.pages[url]
		uid = self.spooler.getNext()
		toReturn = Page(uid, url)
		request, handle = self.openRequest(url)
		self.addHeaders(request)
		if handle:
			try:
				toReturn.content = handle.open(request).read()
				#toReturn.content = unicode(handle.open(request).read(), "utf-8", errors="replace")
				soup = BeautifulSoup(toReturn.content)
				toReturn.title = soup.html.head.title.string
				links = soup('a')
				if DEBUG_FLG:
					print 'page has', len(links), 'links'

			except urllib2.HTTPError, error:
				if error.code == 404:
					print >> sys.stderr, "ERROR: %s -> %s" % (error, error.url)
				else:
					print >> sys.stderr, "ERROR: %s" % error
				links = []
			except urllib2.URLError, error:
				print >> sys.stderr, "ERROR: %s" % error
				links = []
			except AttributeError, error:
				print >> sys.stderr, "ERROR null object"
				links = []

			# process links
			for link in links:
				if not link:
					continue
				href = link.get('href')
				anchor = link.string
				if not anchor:
					anchor = " "
				if not href:
					continue
				#href = urlparse.urlparse(href, '', False).geturl()
				href = self.sanitizeUrl(href)
				fullHref = str(urlparse.urljoin(url, href)) #escape(href)
				fullHref = string.replace(fullHref, '\n', '')
				if self.standardLink(fullHref) and fullHref in self.urls:
					toReturn.addLink(fullHref, anchor)
					if DEBUG_FLG and VERBOSE_FLG:
						print '--> linked to ', fullHref
			return toReturn

	'''
	Save all pages
	'''
	def savePages(self):
		for page in self.pages.itervalues():
			page.savePage()

	'''
	Start the crawling process.
	'''
	def crawl(self, urllist=None):
		if urllist:
			self.urls = set(urllist)
		for url in self.urls:
			self.pages[url] = self.fetchPage(url)
			if DEBUG_FLG:
				print 'Successfully fetched', url
			self.pages[url].savePage()
		if DEBUG_FLG:
			print '******************'
			print 'Finished Fetching!'
		for url, page in self.pages.iteritems():
			for (href, anchor) in page.links:
				if href and anchor:
					terms = anchor.lower().split()
					self.pages[href].addIncomingTerms(terms)

'''
Incidence Matrix

Represent a basic incidence matrix as a two dimensional
NxN python list. Each record is a boolean as to whether
The page in row I links to the page in column J
'''
class IncidenceMatrix:
	def __init__(self, size):
		self.matrix = [[0.0 for i in range(size)] for j in range(size)]	
	
	def at(self, i, j):
		return self.matrix[i][j]

	def writeMatrix(self, filename="incidenceMatrix.txt"):
		with open(filename, "w") as f:
			for row in self.matrix:
				line = []
				for col in row:
					line.append(str(col))
				f.write('  '.join(line) + '\n')

'''
Transition Probability Matrix

Represent a transition probability matrix
Stored as a two dimensional python list of size NxN
'''
class TransitionProbMatrix:
	'''
	Constructor
	Init transition probability matrix
	'''
	def __init__(self, incidenceMatrix):
		self.matrix = copy.deepcopy(incidenceMatrix.matrix)
		N = len(self.matrix)
		dampingFactor = 1.0 - alpha
		teleportProb = alpha / N
		deadEndVal = 1.0 / N
		for i in xrange(N):
			numOnes = 0
			for j in xrange(N):
				if self.matrix[i][j] == 1.0:
					numOnes += 1
			if numOnes > 0:
				for j in xrange(N):
					if self.matrix[i][j] == 1.0:
						self.matrix[i][j] = self.matrix[i][j] / numOnes
					self.matrix[i][j] *= dampingFactor
					self.matrix[i][j] += teleportProb
			else: #deadend
				for j in xrange(N):
					self.matrix[i][j] = deadEndVal
				
	'''
	Write out the matrix to disk
	'''
	def writeMatrix(self, filename="transitionMatrix.txt"):
		with open(filename, "w") as f:
			for row in self.matrix:
				line = []
				for col in row:
					line.append(str(col))
				f.write('  '.join(line) + '\n')

	'''
	Get at a specific point
	'''
	def at(self, i, j):
		return self.matrix[i][j]

'''
PageRank

Class to manage and calculat pagerank data
'''
class PageRank:
	'''
	Constructor
	'''
	def __init__(self, crawler):
		self.crawler = crawler
		self.transitionMatrix = None
		self.N = len(self.crawler.urls)
		self.incidenceMatrix = IncidenceMatrix(self.N)
		self.pageRankVector = [0.0]*self.N
		self.pageRankVector[0] = 1.0

	'''
	Build incidence of pages each pages links to.
	'''
	def buildIncidenceMatrix(self):
		for url, page in self.crawler.pages.iteritems():
			print page.uid
			if DEBUG_FLG:
				print 'processing links in ', url
			linksIndicies = []
			for (linkUrl, anchor) in page.links:
				linksIndicies.append(self.crawler.pages[linkUrl].uid)
			print '--> links to ', len(linksIndicies), 'other pages.'
			for j in linksIndicies:
				self.incidenceMatrix.matrix[page.uid][j] = 1.0


	'''
	Multiply the pagerank vector by the transition probability matrix.
	'''
	def multiplyTransitionMatrix(self):
		vectorNext = []
		for i in range(self.N):
			vectorNext.append(0.0)
		for i in range(self.N):
			for j in range(self.N):
				vectorNext[i] += (self.pageRankVector[j] * self.transitionMatrix.at(j, i))
		return vectorNext

	'''
	Return whether the matrix multiplication on the pagerank vector has converged.
	'''
	def converged(self, vOld):
		for i in range(self.N):
			if math.fabs(vOld[i]-self.pageRankVector[i]) > CONVERGE_ERROR:
				return False
		return True

	'''
	Calculate page rank for all the pages
	'''
	def calcPageRank(self):
		self.buildIncidenceMatrix()
		if DEBUG_FLG:
			self.incidenceMatrix.writeMatrix()

		self.transitionMatrix = TransitionProbMatrix(self.incidenceMatrix)
		if DEBUG_FLG:
			self.transitionMatrix.writeMatrix()

		if DEBUG_FLG:
			print '*********************************************'
			print 'Constructed incidence and transition matrices'
		counter = 0
		vOld = self.pageRankVector
		self.pageRankVector = self.multiplyTransitionMatrix()
		while not self.converged(vOld) or counter < 5:
			vOld = self.pageRankVector
			self.pageRankVector = self.multiplyTransitionMatrix()
			counter += 1
		if DEBUG_FLG:
			print '******************************'
			print 'Multiplied transition matrix ', counter, 'times.'
			print 'PageRank vector has converged.'

	'''
	Write MetaData
	'''
	def writeMetaData(self):
		with codecs.open('metadata', encoding='utf-8', mode='w') as f:
			for page in self.crawler.pages.itervalues():
				# Write out in this format:
				# UID     <page title>     0.053   term1,term2,term3
				termString = ','.join(page.incomingTerms)
				line = [str(page.uid), page.getTitle(), str(self.pageRankVector[page.uid]), termString]
				lineString = '\t'.join(line) + '\n'
				f.write(lineString)
			if DEBUG_FLG:
				print '****************'
				print 'MetaData Written'

'''
Page index Record 
(as specified in course instructions)
Represents a Title and Anchor words refering to the
page with that title
'''
class PageIndexRecord:
	'''
	Constructor
	'''
	def __init__(self, title, anchors=None):
		self.title = title
		self.anchors = anchors

	'''
	Add anchor to the index record.
	'''
	def addAnchor(self, anchorString):
		self.anchors.append(anchorString)

	'''
	Set the title of the index record.
	'''
	def setTitle(self, title):
		self.title = title

'''
Small Index

Holds the small index records per the assignment instructions.
I would ordinarily program this differently and either do an inverted index on words
or have the index records contain more information.
'''
class Index:
	'''
	Constructor
	'''
	def __init__(self):
		# map from uid to index records.
		self.index = {}
		# map from uid to pagerank values
		self.pageRanks = {}
	
	'''
	Add a record from a page object
	'''
	def addPage(self, page):
		self.index[page.uid] = page.getIndexRecord()

	'''
	Add a record from its basic values
	'''
	def addRecord(self, uid, title, anchors):
		self.index[uid] = PageIndexRecord(title, anchors)

	'''
	Find records in the index
	'''
	def findRecords(self, term):
		results = []
		for k, v in self.index.iteritems():
			if term in v.anchors:
				result = (k, v.title, self.pageRanks[k])
				results.append(result)
		return sorted(results, key=operator.itemgetter(2), reverse=True)

	'''
	Build an index from the metadata file
	'''
	def readMetaData(self):
		with codecs.open('metadata', encoding='utf-8', mode='r') as f:
			counter = 0
			previous = None
			for line in f:
				values = string.replace(line, '\n', '').split('\t')
				if len(values) == 4:
					uid = int(values[0])
					title = values[1]
					pageRank = float(values[2])
					anchors = values[3].split(',')
				else:
					if DEBUG_FLG:
						print 'ERROR READING', values
				self.addRecord(uid, title, anchors)
				self.pageRanks[uid] = pageRank
				counter += 1
				previous = values
			if DEBUG_FLG:
				print '***************************'
				print 'Metadata successfully read', counter, 'entries'

'''
Search Class

handle searching and output
'''
class Search:
	'''
	Constructor
	'''
	def __init__(self, index):
		self.index = index

	def getFileName(self, uid):
		return "pages/" + str(uid) + ".html"
	
	'''
	Parse a file for a snippet
	'''
	def parseFile(self, uid):
		filename = self.getFileName(uid)
		with open(filename, 'r') as f:
			soup = BeautifulSoup(f.read())
		return soup

	'''
	Strip tags
	'''
	def strip_tags(self, html):
		text = re.compile(r'<.*?>')
		return text.sub('', html)

	'''
	remove spaces
	'''
	def remove_spaces(self, text):
		s = re.compile(r'\s+')
		toReturn = s.sub(' ', text)
		return string.replace(toReturn, '\n', ' ')

	'''
	Return a snippet from the page
	'''
	def snippet(self, uid):
		soup = self.parseFile(uid)
		title = soup.html.head.title.string
		paragraphs = soup('p')
		for p in paragraphs:
			if p.contents:
				toReturn = self.remove_spaces(self.strip_tags(str(p)))[:140]
				if len(toReturn) < 50:
					continue
				else:
					return toReturn
		return title
	
	'''
	Query a term
	'''
	def query(self, queryString, count=20):
		# (uid, title, pageranks)
		results = self.index.findRecords(queryString)[:10]
		for (uid, title, pagerank) in results:
			print '\n'
			print 'Page:', title
			print 'Rank:', pagerank
			print self.snippet(uid)

########## Main Code ##############
'''
Enumerate urls from file
'''
def openUrlFile(filename="test3.txt"):
	if DEBUG_FLG:
		print 'opening', filename
	urls = []
	with open(filename, 'r') as f:
		for line in f:
			urls.append(string.replace(line, '\n', ''))
	return urls


def printHeader():
	print '******************************'
	print '*** Cornell Library Search ***'
	print '******************************'

'''
Run
'''
def Run():
	if len(sys.argv) > 2:
		# create a crawler with the pages we want to read
		crawler = WebCrawler(openUrlFile(sys.argv[2]))
		crawler.crawl()
		# initialize a pagerank calculator to build the web graph from our pages
		pageRank = PageRank(crawler)
		# calulate the pagerank for our graph
		pageRank.calcPageRank()
		# write out the data to the metadata file.
		pageRank.writeMetaData()
	
	if '-v' in sys.argv:
		global VERBOSE_FLG
		VERBOSE_FLG = True

	# get our index
	index = Index()
	# Read in from the metadata file.
	index.readMetaData()
	search = Search(index)
	printHeader()
	while True:
		print "\n"
		query = raw_input("Enter a search: ")
		if query == 'ZZZ':
			break
		elif query == "":
			continue
		elif query in STOPWORDS:
			print "Note: '"+query+"' is a frequently occuring word. Please input a more specific query."
			continue
		else:
			search.query(query)
		

'''
Bootstrap
'''
if __name__ == "__main__":
	Run()