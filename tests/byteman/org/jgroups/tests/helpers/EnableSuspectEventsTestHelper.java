package org.jgroups.tests.helpers;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Byteman helper for EnableSuspectEventsDeterministicEventTest.
 * <p>
 * Reproduces the stale-connection race deterministically using a rendezvous between
 * the peer's retry-connect (from retained bundler messages) and the rejoiner's outgoing
 * connection.
 * <p>
 * After the rejoiner disconnects, the peers' bundlers retain unsent messages. When the
 * rejoiner's TCP server restarts, the peers retry and create outgoing connections to the
 * rejoiner ({@code getConnection} → {@code handleOutgoingConnection}). The rendezvous
 * delays the rejoiner's own outgoing {@code handleOutgoingConnection} to a peer until
 * that peer has already connected to the rejoiner. When the rejoiner's outgoing arrives
 * at the peer's acceptor, the peer rejects it (connection already exists), closing the
 * rejoiner's outgoing socket — reproducing the dead-connection race.
 */
public class EnableSuspectEventsTestHelper extends Helper {

    private static final AtomicBoolean armed = new AtomicBoolean(false);
    private static final AtomicBoolean fired = new AtomicBoolean(false);
    private static final AtomicReference<String> armedNodeName = new AtomicReference<>();

    /** Signaled when a peer has established an outgoing connection TO the rejoiner */
    private static volatile CountDownLatch peerConnectedToRejoiner;

    protected EnableSuspectEventsTestHelper(Rule rule) {
        super(rule);
    }

    /** Arm the rendezvous for the given rejoining node name (e.g. "A", "B", "C") */
    public static void arm(String nodeName) {
        peerConnectedToRejoiner = new CountDownLatch(1);
        fired.set(false);
        armedNodeName.set(nodeName);
        armed.set(true);
    }

    public static void disarm() {
        armed.set(false);
        fired.set(false);
        armedNodeName.set(null);
        CountDownLatch l = peerConnectedToRejoiner;
        if(l != null) l.countDown();
    }

    /**
     * Called by Byteman at replaceConnection entry.
     * <p>
     * On a PEER's bundler thread connecting TO the rejoiner: signals that the peer
     * has established an outgoing connection to the rejoiner. Does NOT block.
     * <p>
     * On the REJOINER's bundler thread connecting to a peer: waits until a peer has
     * connected to the rejoiner first, ensuring the peer already has {@code conns[rejoiner]}
     * when the rejoiner's incoming connection arrives — causing the peer to reject it.
     */
    public static void maybePause(Object node, Object dest, Object conn, String threadName) {
        if(!armed.get())
            return;
        String targetNode = armedNodeName.get();
        if(targetNode == null)
            return;
        if(!threadName.contains("pd-bundler"))
            return;

        boolean isRejoiner = threadName.endsWith("," + targetNode);
        int targetPort = 7800 + (targetNode.charAt(0) - 'A');
        String destStr = String.valueOf(dest);
        boolean destIsRejoiner = destStr.contains(":" + targetPort);

        if(!isRejoiner && destIsRejoiner) {
            // Peer's bundler connecting TO the rejoiner (from retained message retry)
            System.out.println("--> [RENDEZVOUS] peer connected to rejoiner dest=" + dest
                               + " node=" + node + " thread=" + threadName);
            peerConnectedToRejoiner.countDown();
        }
        else if(isRejoiner && !destIsRejoiner) {
            // Rejoiner's bundler connecting to a peer — wait for the peer to connect first
            if(!fired.compareAndSet(false, true))
                return;
            System.out.println("--> [RENDEZVOUS] rejoiner waiting for peer to connect first dest=" + dest
                               + " node=" + node + " thread=" + threadName);
            try {
                peerConnectedToRejoiner.await(5, TimeUnit.SECONDS);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("--> [RENDEZVOUS] rejoiner proceeding dest=" + dest + " node=" + node);
        }
    }
}
