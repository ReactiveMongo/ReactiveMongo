package reactivemongo.jmx;

import java.util.List;

/**
 * The specification of the MBean about the ReactiveMongo node set.
 * Instances will be registered with names of the form
 * `org.reactivemongo.{supervisor}.{connection}:type=NodeSet`.
 */
public interface NodeSetMBean {

    /**
     * The connection options.
     *
     * @return The string representation of the connection options
     */
    public String getConnectionOptions();

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
     * The optional name of the node set (replicaSet).
     *
     * @return The node set name if any, or null of none
     */
    public String getName();

    /**
     * The node set version.
     *
     * @return The version of the node set if any, or -1 if none
     */
    public long getVersion();

    /**
     * The name of the primary node.
     *
     * @return The primary name if any, or null if none
     */
    public String getPrimary();

    /**
     * The name of the mongos node.
     *
     * @return The mongos name if any, or null if none
     */
    public String getMongos();

    /**
     * The name of the nearest node.
     *
     * @return The nearest name if any, or null if none
     */
    public String getNearest();

    /**
     * The information about all the nodes.
     *
     * @return The string representation for all the nodes (possibly empty)
     */
    public String getNodes();

    /**
     * The information about the secondary nodes.
     *
     * @return The string representation of the secondary nodes (possible empty)
     */
    public String getSecondaries();
}
