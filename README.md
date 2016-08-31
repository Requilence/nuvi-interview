NUVI INTERVIEW CODE PROJECT
===========

You can override default settings with some environment variables:
- **NUVI_LIST_LINK** - URL to directory listing
- **NUVI_POOL_DOWNLOADS** - the number of maximum parallel downloads
- **NUVI_POOL_PROCESSING** - the number of maximum parallel data processing (unzip + redis)
  
In addition to **NEWS_XML** Redis list my solution additionally uses:
-  Redis hash **NEWS_XML_KEY** to store inserted XMLs hash sums(in this case MD5 of the content obtained from the filename)
-  Redis hash **NEWS_ZIP_URL** to store downloaded and processed URLs

The application is idempotent. It can be restarted and it will skip already downloaded&processed files. 
It also checks XML hash sum before pushing to **NEWS_XML_KEY** because the provided files source contains a lot of duplicate XML's.

### Requirements

Go 1.5+, Redis 2.0.0+
