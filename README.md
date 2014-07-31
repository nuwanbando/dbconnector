dbconnector : Sample Sequence / Usage
=======================================

<sequence xmlns="http://ws.apache.org/ns/synapse" name="dbconnectorSeq">
   <dbconnector.query>
      <dbUser>root</dbUser>
      <dbPass>root</dbPass>
      <jdbcURL>jdbc:mysql://localhost:3306/book</jdbcURL>
      <jdbcDriver>com.mysql.jdbc.Driver</jdbcDriver>
      <query>select * from books where id=? and name=?</query>
      <params>{fn:concat('INTEGER,', $url:id, ';', 'VARCHAR,', $url:name)}</params>
   </dbconnector.query>
   <log level="full">
      <property name="CONNECTOR_LOG1" value="#### AFTER QUERY EXE #######"></property>
   </log>
   <property name="messageType" value="application/json" scope="axis2"></property>
   <property name="NO_ENTITY_BODY" action="remove" scope="axis2"></property>
   <respond></respond>
</sequence>
