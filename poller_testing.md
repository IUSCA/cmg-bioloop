- I need to test the poller scripts for the cmg migration. for this, i have created this fresh clone of this app.

- I have a version of CMG running on localhost, which you will be able to make changes to at the  DB or API level, whatever is needed. You will change the data in my local CMG instance, and then make sure that those changes appear in this app's DB. You will also have read/write/execute access for all docker/compose cmds on this localhost for this testing. You are allowed all access to CMG's docker containers as well.

- Eventually we will need to simulate parallel activity by multiple users. For now, just do simple checks - for example:
* a new dataset being created in cmg should also be picked up this app's dataset-lookout proces (which is the @workers/workers/scripts/watch.py script, which kicks off the Integrated wf)
* we will create the dataset in local CMG first, then make sure that some point, it's Integrated wf finishes in bioloop (API route /workflows and /datasets).
* future tests (start documenting these for now ONLY):
a.) Staging kicked off in CMG should also be kicked off in bioloop for corresponding dataset
b.) Conversion kicked off in CMG should also be kicked off in bioloop for corresponding dataset

- would prefer to simulate through scripting

- add extensive logging to scripts - i don't want to have to re-run a test just because i had to add logging to see what went wrong.
