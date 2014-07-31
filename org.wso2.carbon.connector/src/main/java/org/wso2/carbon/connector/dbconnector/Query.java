/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.connector.dbconnector;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.datasource.DataSourceInformation;
import org.apache.synapse.commons.datasource.factory.DataSourceFactory;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.securevault.secret.SecretInformation;

import javax.sql.DataSource;
import javax.xml.stream.XMLStreamException;
import java.math.BigDecimal;
import java.sql.*;

public class Query extends AbstractConnector {

    private Log log = LogFactory.getLog(Query.class);
    private String dbUser;
    private String dbPass;
    private String jdbcURL;
    private String jdbcDriver;
    private String query;
    private String params;
    private String[] paramsArr;

    private DataSourceInformation dataSourceInformation = null;
    private Connection conn = null;


    public void connect(MessageContext messageContext) throws ConnectException {
        try {
            dbUser = (String) getParameter(messageContext, "dbUser");
            dbPass = (String) getParameter(messageContext, "dbPass");
            jdbcURL = (String) getParameter(messageContext, "jdbcURL");
            jdbcDriver = (String) getParameter(messageContext, "jdbcDriver");
            query = (String) getParameter(messageContext, "query");
            params = (String) getParameter(messageContext, "params");
            //System.out.println(params);
            paramsArr = params.split(";");

            dataSourceInformation = getDataSourceInformation();
            if (execute(messageContext)) {
                log.info("DBConnector query executed successfully.");
            } else {
                log.warn("DBConnector failed to execute query.");
            }

        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    private int getType(String type) {
        if ("CHAR".equals(type)) {
            return Types.CHAR;
        } else if ("VARCHAR".equals(type)) {
            return Types.VARCHAR;
        } else if ("LONGVARCHAR".equals(type)) {
            return Types.LONGVARCHAR;
        } else if ("NUMERIC".equals(type)) {
            return Types.NUMERIC;
        } else if ("DECIMAL".equals(type)) {
            return Types.DECIMAL;
        } else if ("BIT".equals(type)) {
            return Types.BIT;
        } else if ("TINYINT".equals(type)) {
            return Types.TINYINT;
        } else if ("SMALLINT".equals(type)) {
            return Types.SMALLINT;
        } else if ("INTEGER".equals(type)) {
            return Types.INTEGER;
        } else if ("BIGINT".equals(type)) {
            return Types.BIGINT;
        } else if ("REAL".equals(type)) {
            return Types.REAL;
        } else if ("FLOAT".equals(type)) {
            return Types.FLOAT;
        } else if ("DOUBLE".equals(type)) {
            return Types.DOUBLE;
        } else if ("DATE".equals(type)) {
            return Types.DATE;
        } else if ("TIME".equals(type)) {
            return Types.TIME;
        } else if ("TIMESTAMP".equals(type)) {
            return Types.TIMESTAMP;
        } else {
            throw new SynapseException("Unknown or unsupported JDBC type : " + type);
        }
    }

    private PreparedStatement getPreparedStatement() {
        PreparedStatement ps = null;
        int column = 1;
        try {
            ps = conn.prepareStatement(query);

            for (String p : paramsArr) {
                String[] param = p.split(",");
                String type = param[0];
                String value = param[1];

                switch (getType(type)) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR: {
                        if (value != null && value.length() != 0) {
                            ps.setString(column++, value);
                        } else {
                            ps.setString(column++, null);
                        }
                        break;
                    }
                    case Types.NUMERIC:
                    case Types.DECIMAL: {
                        if (value != null && value.length() != 0) {
                            ps.setBigDecimal(column++, new BigDecimal(value));
                        } else {
                            ps.setBigDecimal(column++, null);
                        }
                        break;
                    }
                    case Types.BIT: {
                        if (value != null && value.length() != 0) {
                            ps.setBoolean(column++, Boolean.parseBoolean(value));
                        } else {
                            ps.setNull(column++, Types.BIT);
                        }
                        break;
                    }
                    case Types.TINYINT: {
                        if (value != null && value.length() != 0) {
                            ps.setByte(column++, Byte.parseByte(value));
                        } else {
                            ps.setNull(column++, Types.TINYINT);
                        }
                        break;
                    }
                    case Types.SMALLINT: {
                        if (value != null && value.length() != 0) {
                            ps.setShort(column++, Short.parseShort(value));
                        } else {
                            ps.setNull(column++, Types.SMALLINT);
                        }
                        break;
                    }
                    case Types.INTEGER: {
                        if (value != null && value.length() != 0) {
                            ps.setInt(column++, Integer.parseInt(value));
                        } else {
                            ps.setNull(column++, Types.INTEGER);
                        }
                        break;
                    }
                    case Types.BIGINT: {
                        if (value != null && value.length() != 0) {
                            ps.setLong(column++, Long.parseLong(value));
                        } else {
                            ps.setNull(column++, Types.BIGINT);
                        }
                        break;
                    }
                    case Types.REAL: {
                        if (value != null && value.length() != 0) {
                            ps.setFloat(column++, Float.parseFloat(value));
                        } else {
                            ps.setNull(column++, Types.REAL);
                        }
                        break;
                    }
                    case Types.FLOAT: {
                        if (value != null && value.length() != 0) {
                            ps.setDouble(column++, Double.parseDouble(value));
                        } else {
                            ps.setNull(column++, Types.FLOAT);
                        }
                        break;
                    }
                    case Types.DOUBLE: {
                        if (value != null && value.length() != 0) {
                            ps.setDouble(column++, Double.parseDouble(value));
                        } else {
                            ps.setNull(column++, Types.DOUBLE);
                        }
                        break;
                    }
                    // skip BINARY, VARBINARY and LONGVARBINARY
                    case Types.DATE: {
                        if (value != null && value.length() != 0) {
                            ps.setDate(column++, Date.valueOf(value));
                        } else {
                            ps.setNull(column++, Types.DATE);
                        }
                        break;
                    }
                    case Types.TIME: {
                        if (value != null && value.length() != 0) {
                            ps.setTime(column++, Time.valueOf(value));
                        } else {
                            ps.setNull(column++, Types.TIME);
                        }
                        break;
                    }
                    case Types.TIMESTAMP: {
                        if (value != null && value.length() != 0) {
                            ps.setTimestamp(column++, Timestamp.valueOf(value));
                        } else {
                            ps.setNull(column++, Types.TIMESTAMP);
                        }
                        break;
                    }
                    // skip CLOB, BLOB, ARRAY, DISTINCT, STRUCT, REF, JAVA_OBJECT
                    default: {
                        String msg = "Trying to set an un-supported JDBC Type : " + getType(type) +
                                " against column : " + column + " and statement : " +
                                query +
                                " used by a DB mediator against DataSource : " + jdbcURL +
                                " (see java.sql.Types for valid type values)";
                        log.error(msg);
                    }

                }
            }


        } catch (SQLException e) {
            log.error("There was a SQL error. " + e.getMessage(), e);
        }

        return ps;
    }

    public boolean execute(MessageContext messageContext) {
        PreparedStatement ps = null;
        DataSource dataSource = DataSourceFactory.createDataSource(dataSourceInformation);

        try {

            conn = dataSource.getConnection();
            ps = getPreparedStatement();

            ResultSet rs = ps.executeQuery();
            OMElement body = AXIOMUtil.stringToOM("<RESULTS xmlns:dbc=\"http://wso2.org/connector/dbconnector/results\"></RESULTS>");

            if (messageContext.getEnvelope().getBody().getFirstElement() != null) {
                messageContext.getEnvelope().getBody().getFirstElement().detach();
            }

            messageContext.getEnvelope().getBody().addChild(body);

            int count = 0;
            int colCount = rs.getMetaData().getColumnCount();
            String colName = null;
            String value = null;
            OMElement row = null;
            while (rs.next()) {
                OMElement eleRow = AXIOMUtil.stringToOM("<ROW xmlns:dbc=\"http://wso2.org/connector/dbconnector/row\"></ROW>");
                body.addChild(eleRow);
                for (int i = 1; i <= colCount; i++) {
                    colName = rs.getMetaData().getColumnName(i);
                    row = AXIOMUtil.stringToOM("<" + colName + "/>");
                    if (rs.getBytes(i) != null) {
                        //if (getStringType(rs.getMetaData().getColumnClassName(i))) {
                            value = new String(rs.getBytes(i));
                        //} else {
                          //  value = new sun.misc.BASE64Encoder().encode(rs.getBytes(i));
                        //}
                    } else {
                        value = "";
                    }
                    row.setText(value);
                    eleRow.addChild(row);
                }
                count++;
            }

            log.info("DBConnector returned " + count + " results.");

            return true;

        } catch (SQLException e) {
            log.error("There was a SQL error. " + e.getMessage(), e);
        } catch (XMLStreamException e) {
            log.error("DBConnector error serialising query result.");
        }

        return false;
    }

    private boolean getStringType(String colName) {
        if (colName.startsWith("java.lang")) {
            return true;
        }

        if (colName.startsWith("java.sql.Time")) {
            return true;
        }

        if (colName.startsWith("java.sql.Date")) {
            return true;
        }

        if (colName.startsWith("java.sql.Blob") || colName.startsWith("java.sql.Clob") || colName.startsWith("java.sql.NClob")) {
            return false;
        }

        return false;
    }

    private DataSourceInformation getDataSourceInformation() {
        DataSourceInformation dataSourceInformation = new DataSourceInformation();
        dataSourceInformation.setUrl(jdbcURL);
        dataSourceInformation.setDriver(jdbcDriver);

        SecretInformation secretInformation = new SecretInformation();
        secretInformation.setUser(dbUser);
        secretInformation.setAliasSecret(dbPass);

        dataSourceInformation.setSecretInformation(secretInformation);

        return dataSourceInformation;
    }

}
