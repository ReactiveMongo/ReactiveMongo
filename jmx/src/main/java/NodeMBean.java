package reactivemongo.jmx;

/**
 * The specification of the MBean about a ReactiveMongo node.
 * Instances will be registered with names of the form 
 * `org.reactivemongo.{supervisor}.{connection}:type=Node,name={host}-{port}`.
 */
public interface NodeMBean {

    /**
     * The name of the pool supervisor.
     *
     * @return The supervisor name
     */
    public String getSupervisor();

    /**
     * The name of the connection pool.
     *
     * @return The connection name
     */
    public String getConnection();

    /**
     * The name of the node.
     *
     * @return the node name
     */
    public String getName();

    /**
     * The aliases of the node.
     *
     * @return the node aliases as comma separated string
     */
    public String getAliases();

    /**
     * The name of the node host.
     *
     * @return the node hostname
     */
    public String getHost();

    /**
     * The MongoDB port on the node.
     *
     * @return the node port
     */
    public int getPort();

    /**
     * The status of the node.
     *
     * @return the string representation of the node status
     * @see reactivemongo.core.nodeset.NodeStatus
     */
    public String getStatus();

    /**
     * The number of connections managed for this node.
     *
     * @return the connection count
     */
    public int getConnections();

    /**
     * The number of connected channels (established connections) for this node.
     *
     * @return the connected count
     * @see #getConnections()
     */
    public int getConnected();

    /**
     * The number of authenticated connections for this node.
     *
     * @return the count of authenticated connections
     * @see #getConnections()
     */
    public int getAuthenticated();

    /**
     * The tags for this node.
     *
     * @return the string representation of the node tags, or null if node
     */
    public String getTags();

    /**
     * The protocol metadata for the node connections.
     *
     * @return the string representation of the protocol metadata
     */
    public String getProtocolMetadata();

    /**
     * The information about the node ping.
     *
     * @return the string representation of the node ping
     */
    public String getPingInfo();

    /**
     * Indicates whether the node is a Mongos one.
     *
     * @return true if the node is mongos, otherwise false
     */
    public boolean isMongos();
}
