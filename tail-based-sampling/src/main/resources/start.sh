echo "starting app..."
if [ $SERVER_PORT ]
then
echo "starting app with port $SERVER_PORT"
result=`ps -ef | grep "-Dserver.port=$SERVER_PORT" | grep -v status | grep -v grep`
		if [ -z "$result" ]; then
			echo "tailbaseSampling-1.0-SNAPSHOT.jar is not running. Nothing to stop."
		else
			echo "tailbaseSampling-1.0-SNAPSHOT.jar is running. Going to stop now."
            	printf "\t%s" $result 
			ps -ef | grep "-Dserver.port=$SERVER_PORT" | grep -v status | grep -v grep | awk '{print $2}' | xargs kill -9
		fi
		printf "\n"
   /usr/local/src/jdk1.8.0_11/bin/java -Dserver.port=$SERVER_PORT -jar /usr/local/src/tailbaseSampling-1.0-SNAPSHOT.jar &
else
   /usr/local/src/jdk1.8.0_11/bin/java -Dserver.port=8000 -jar /usr/local/src/tailbaseSampling-1.0-SNAPSHOT.jar &
fi
tail -f /usr/local/src/start.sh