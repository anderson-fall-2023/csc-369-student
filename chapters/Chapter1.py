# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,md,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.8.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] slideshow={"slide_type": "slide"} hideCode=false hidePrompt=false
# # Chapter 1
#
# ## Cloud computing and distributed file systems
#
# Paul E. Anderson

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Ice Breaker
#
# What's your best holiday gift ever?

# + [markdown] slideshow={"slide_type": "subslide"}
# While this text can be viewed as PDF, it is most useful to have a Jupyter environment. I have an environment ready for each of you, but you can get your own local environment going in several ways. One popular way is with Anaconda (<a href="https://www.anaconda.com/">https://www.anaconda.com/</a>. Because of the limited time, you can use my server.

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Preamble
# Hello and welcome to an introduction to distributed computing. While there are many ways to approach this subject from both a practical and historical perspective, we are restricting ourselves to a view of distributed computing that attempts to build efficient solutions to typical data scienceproblems that require distributed computing. For the majority of this class this involves solutions that relate to the data science pipeline:
# 1. Obtaining data
# 2. Scrubbing data
# 3. Exploring data
# 4. Modeling data
# 5. Interpreting data

# + [markdown] slideshow={"slide_type": "subslide"}
# * We will not and cannot attempt to cover data science in addition to distributed computing. 
# * From our perspective, we will focus on the distributed computing technologies necessary for modern data science. 

# + [markdown] slideshow={"slide_type": "slide"}
# ## Shortest of Short History Lessons
# For our discussion, the world did not exist prior to the 1980s. So our story begins with the rise of the client/server model connected by the internet. This is our most familiar distributed system, and it is shown below:
#
# <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/c/c9/Client-server-model.svg/1200px-Client-server-model.svg.png" width=300>
#
# While an entire distributed system course could be taught around different aspects of this picture, we will again refocus back to data science. In other words, we view distributed systems as a tool to help us solve data science and related activities. As most of the world is now data-driven, this is not a stretch for many backgrounds even if your never destinated to be a data scientist or data engineer.

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Distributed Computing
#
# As is the case in many fields, the term distributed computing has varied defintions. For this course, we will discuss both loosely coupled distributed systems and tightly coupled distributed systems. The terms "distributed computing", "parallel computing", and "concurrent computing" all have some overlap though distinctions are often made in context. An example of a loosely coupled distributed system is the client-server model shown. An example of a tightly coupled distributed system is performing a parallel computation on two cpus (or cores) in a single computer. 

# + [markdown] slideshow={"slide_type": "subslide"}
# ### So what history is relevant to this class?
# <img src="https://s2.studylib.net/store/data/014193816_1-c992dbd11a019db364ebc6c5cbc55e2d.png" width=700>

# + [markdown] slideshow={"slide_type": "slide"}
# ## Never tell me about your implementation concerns!
#
# <img src="https://media.tenor.com/images/0795d63faba1aeb2348eed9d24c78bc6/tenor.png" width=500>

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Fallacies of distributed computing
# We will ignore when it is convienent the following falacies:
# 1. The network is reliable
# 2. Latency is zero
# 3. Bandwidth is infinite
# 4. The network is secure
# 5. Topology does not change
# 6. There is one administrator
# 7. Transport cost is zero
# 8. The network is homogenous
#
# Source: Arnon Rotem-Gal-Oz, Fallacies of Distributed Computing Explained,
# white paper, http://www.rgoarchitects.com/Files/fallacies.pdf.
#
#

# + [markdown] slideshow={"slide_type": "slide"}
# ## Why distributed computing?
#
# The first answer to this question is what do you mean by distributed. Everyone thinks of CPU advances over the years, but don't forget other hardware advances have arrived:
#
# <img src="https://techtalk.pcmatic.com/graph_lib/research/rc_mem_avgmem_pctype.php" width=300>
#
# It is very important to keep in mind that a problem that needed one form of distributed computing in the past, may not need the same form of distributed computing today. 
#
# Our answer to this question is:
#
# **We build distributed systems to build more efficient and optimized solutions to solve problems of interest.**

# + [markdown] slideshow={"slide_type": "subslide"}
# ### How are distributed systems different?
# While we can abstract away some of elements of distributed computing, we are going to study approaches for:
# 1. How to store data on multiple systems?
# 2. How to handle updates and fix (or handle) inconsistencies?
# 3. How do we assemble the full answer?

# + [markdown] slideshow={"slide_type": "slide"}
# ## Practical Considerations
# Most real world examples that need distributed computing need distributed computing because they would otherwise (and may still) require a long time to run. This isn't practical for learning. Even if we weren't in a learning mode, we would still focus on small case studies. Why? Even in the real world we test on small subsets of data before scaling up. All of the examples throughout this class are scaled down representations of a real problem that may require distributed computing depending on time and resources. We have resources available if/when your project requires such a system.
# -

# ## The Cloud

# <img src="https://timesofcloud.com/wp-content/uploads/2015/02/The-History-of-the-Cloud-1024x511.png">

# What people broadly define as the cloud isn't useful for anyone other than marketing folks making commercials, so what could it mean to us?
#
# <img src="https://images.ctfassets.net/9ijwdiuuvngh/6qfMq0bifK06q6I4GyyOEa/6278fd088cd22cd97659fbff2c424e25/Bp_Cloud_en.png">

# **Problem 1:** Sign up for a free student account on Microsoft Azure: https://azure.microsoft.com/en-us/free/students/.
#
# Why Azure? There is not credit card signup required, and it is my second favorite cloud provider. 
#
# **Your solution here: Embed an image showing you now have an account**

# ### Virtual Machines
#
# <img src="https://miro.medium.com/max/1838/0*NP_Pmdq7lCQB_L1j.png">

# **Problem 2:** Create a virtual machine and connect over ssh. 
#
# A video walkthrough is available here: <a href="https://web.microsoftstream.com/video/376a6143-1425-413d-939b-7cf8773bf7b5">https://web.microsoftstream.com/video/376a6143-1425-413d-939b-7cf8773bf7b5</a>.
#
# **Your solution here: Embed an image showing you now have an account**

# ## Distributed File Systems

# ### NFS - one of the oldest and most famous
#
# <img src="https://media.geeksforgeeks.org/wp-content/uploads/20200711142821/DFS.png">

# We will not worry about a lot of the implementation details for distributed file systems, but to give you a taste here is an excerpt from the Wikipedia page:
#
# Assuming a Unix-style scenario in which one machine (the client) needs access to data stored on another machine (the NFS server):
# * The server implements NFS daemon processes, running by default as nfsd, to make its data generically available to clients.
# * The server administrator determines what to make available, exporting the names and parameters of directories, typically using the /etc/exports configuration file and the exportfs command.
# * The server security-administration ensures that it can recognize and approve validated clients.
# * The server network configuration ensures that appropriate clients can negotiate with it through any firewall system.
# * The client machine requests access to exported data, typically by issuing a mount command. (The client asks the server (rpcbind) which port the NFS server is using, the client connects to the NFS server (nfsd), nfsd passes the request to mountd)
# * If all goes well, users on the client machine can then view and interact with mounted filesystems on the server within the parameters permitted.

# Why is NFS not suitable for many data science applications?
#
# The single biggest problem: NFS is designed for a single centralized server, not for scale-out. Many alternatives exist, and during this class we will discuss several. One example we will discuss in this lecture, and you will get hands on exposure to in lab is Gluster.
#
# https://youtu.be/wo4gNX608U0

# ### Gluster
# > GlusterFS is a scale-out network-attached storage file system. It has found applications including cloud computing, streaming media services, and content delivery networks. GlusterFS was developed originally by Gluster, Inc. and then by Red Hat, Inc., as a result of Red Hat acquiring Gluster in 2011. Source: https://en.wikipedia.org/wiki/Gluster
#
# <img src="https://docs.gluster.org/en/latest/images/New-DistributedVol.png">

# ### Key traits
# * Scalable,
# * Capable of scaling to several brontobytes (10^27) and thousands of clients
# * Designed for commodity servers and storage to form massive storage networks 

# ### Other features
# * Highly available storage
# * Built in replication and geo-replication
# * Self-healing
# * The ability to re-balance data 

# ### Definitions
# * Bricks - storage units which consist of a server and directory path (i.e., server:/export)
# * Trusted Storage Pool – a trusted network of servers that will host storage resources
# * Volumes - collection of bricks with a common redundancy requirement 

# ### Gluster Volume Types
# Gluster supports a number of volumes types, each providing different availability and performance characteristics:
# * Distributed – Files are distributed across bricks in the cluster
# * Replicated – Files are replicated across one or more bricks in the cluster
# * Striped – Stripes data across one or more bricks
# * Distributed replicated – Distributes files across replicated bricks in a cluster
# * Distributed striped – Stripes data across two or more nodes in the cluster

# ### Performance Considerations
# From Gluster documentation:
# * Use distributed volumes where the requirement is to scale storage and the redundancy is either not important or is  provided by other hardware/software layers
# * Use replicated volumes in environments where highavailability and high-reliability are critical
# * Use striped volumes only in high concurrency environments accessing very large files
# * Use distributed striped volumes where the requirement is to scale storage and in high concurrency environments accessing very large files
# * Use distributed replicated volumes in environments where the requirement is to scale storage and high-reliability is critical. Distributed replicated volumes offer improved read performance in most environments

# ## Conclusion
# Gluster is one of many distributed file systems that are used to organize and access large datasets and files. We'll discuss additional distributed file systems such as the Hadoop Distributed File System in subsequent chapters. Gluster was chosen because of its community support and industry adoption. Its goal is to provide a file system for applications needing scale-out storage and high-reliability. In other words, it is designed for applications that do not fit the traditional client-server model of file systems such as NFS.


