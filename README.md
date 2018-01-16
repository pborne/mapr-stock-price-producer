# Bluecoat messaging
Simple producer & consumer that populates a stream with bluecoat messages

######################################
# On the cluster, as the user 'mapr' #
######################################

# Delete the existing stream if needed
maprcli stream delete -path /mapr/demo.mapr.com/projects/project2/bluecoat

# Create a new stream if it does not exist already
maprcli stream create -path /mapr/demo.mapr.com/projects/project2/bluecoat -ttl 0 -autocreate true -compression off -produceperm u:user1 -copyperm u:user1 -adminperm u:user1 -topicperm u:user1 -consumeperm u:user1

#################
# On the Client #
#################
# Make sure we can see the stream:
ls -l /mapr/demo.mapr.com/projects/project2/
total 1

drwxr-xr-x 3 user1 tenant1 2 Oct 17  2016 2001
lr-------- 1 mapr  mapr    3 Jan 13 06:42 bluecoat -> mapr::table::2187.21524.184284

# Start the Consumer
java -cp /home/user1/bluecoat.jar com.mapr.demo.bluecoat.Consumer /mapr/demo.mapr.com/projects/project2/bluecoat:topic1

# In another session, start the Producer
java -cp /home/user1/bluecoat.jar com.mapr.demo.bluecoat.Producer /mapr/demo.mapr.com/projects/project2/bluecoat:topic1 /home/user1/bluecoat/bluecoat_00.csv.gz

