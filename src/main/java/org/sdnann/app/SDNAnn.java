package org.sdnann.app;

import org.onlab.osgi.DefaultServiceDirectory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.DefaultLink;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Link;
import org.onosproject.net.MastershipRole;
import org.onosproject.net.Port;
import org.onosproject.net.device.DefaultDeviceDescription;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceProvider;
import org.onosproject.net.device.DeviceProviderRegistry;
import org.onosproject.net.device.DeviceProviderService;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.link.DefaultLinkDescription;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkProvider;
import org.onosproject.net.link.LinkProviderRegistry;
import org.onosproject.net.link.LinkProviderService;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.provider.AbstractProvider;
import org.onosproject.net.provider.ProviderId;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.HashMap;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * App that subscribes to device/link events and sets annotations.
 */
@Component(immediate = true)
public class SDNAnn {

    private final Logger log = getLogger(getClass());

    static final ProviderId PID = new ProviderId("app", "org.sdnann.app", true);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LinkService linkService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ComponentConfigService cfgService;

    private ApplicationId appId;

    private DeviceProviderRegistry deviceRegistry;
    private DeviceProvider deviceProvider;
    private DeviceProviderService deviceProviderService;

    private DevListener devListener;

    private LinkProviderRegistry linkRegistry;
    private LinkProvider linkProvider;
    private LinkProviderService linkProviderService;

    private LnkListener lnkListener;

    private Map<ConnectPoint, ConnectPoint> linkMap;

    private void initLinkMap() {
        linkMap = new HashMap<>();
    }

    @Activate
    public void activate(ComponentContext context) {
        appId = coreService.registerApplication("org.sdnann.app");

        initLinkMap();

        deviceRegistry = DefaultServiceDirectory.getService(DeviceProviderRegistry.class);
        deviceProvider = new DeviceAnnotationProvider();
        deviceProviderService = deviceRegistry.register(deviceProvider);

        devListener = new DevListener();
        deviceService.addListener(devListener);

        linkRegistry = DefaultServiceDirectory.getService(LinkProviderRegistry.class);
        linkProvider = new LinkAnnotationProvider();
        linkProviderService = linkRegistry.register(linkProvider);

        lnkListener = new LnkListener();
        linkService.addListener(lnkListener);

        annotateDevices();
        annotateLinks();

        log.info("Started with Application ID {}", appId.id());
    }

    @Deactivate
    public void deactivate() {
        deviceService.removeListener(devListener);
        deviceRegistry.unregister(deviceProvider);
        log.info("Stopped");
    }

    @Modified
    public void modified(ComponentContext context) {
    }

    private void annotateDevices() {
        for (Device device : deviceService.getDevices()) {
            annotateDevice(device);
            for (Port port : deviceService.getPorts(device.id())) {
                addPortLinks(port);
            }
        }
    }

    private void annotateDevice(Device device) {
        DefaultAnnotations.Builder builder = DefaultAnnotations.builder();

        // fill in annotation
        builder.set(AnnotationKeys.LATITUDE, "32.7150");
        builder.set(AnnotationKeys.LONGITUDE, "117.1625");

        DefaultDeviceDescription description = new DefaultDeviceDescription(device.id().uri(),
                device.type(), device.manufacturer(), device.hwVersion(), device.swVersion(),
                device.serialNumber(), device.chassisId(), builder.build());
        log.info("Annotating dev: {}, des: {}", device, description);
        deviceProviderService.deviceConnected(device.id(), description);
    }

    private void annotateLinks() {
        for (Link link : linkService.getLinks()) {
            annotateLink(link);
        }
    }

    private void annotateLink(Link link) {
        DefaultAnnotations.Builder builder = DefaultAnnotations.builder();

        // fill in annotation
        builder.set(AnnotationKeys.LATENCY, "10");
        Port srcp = deviceService.getPort(link.src().deviceId(), link.src().port());
        Port dstp = deviceService.getPort(link.dst().deviceId(), link.dst().port());
        double speed = Math.min(srcp.portSpeed(), dstp.portSpeed());
        builder.set(AnnotationKeys.BANDWIDTH, String.valueOf(speed));

        DefaultLinkDescription description = new DefaultLinkDescription(link.src(), link.dst(),
                link.type(), builder.build());
        log.info("Annotation link: {}, des: {}", link, description);
        linkProviderService.linkDetected(description);
    }

    private class DevListener implements DeviceListener {
        @Override
        public void event(DeviceEvent event) {
            if (event.type() == DeviceEvent.Type.DEVICE_ADDED) {
                annotateDevice(event.subject());
            }

            if (event.type() == DeviceEvent.Type.DEVICE_ADDED ||
                    event.type() == DeviceEvent.Type.PORT_ADDED ||
                    event.type() == DeviceEvent.Type.PORT_UPDATED) {
                for (Port port : deviceService.getPorts(event.subject().id())) {
                    addPortLinks(port);
                }
            }
        }
    }

    private void addPortLinks(Port port) {
        if (!port.isEnabled()) {
            return;
        }
        ConnectPoint cp = new ConnectPoint(port.element().id(), port.number());
        if (linkMap.containsKey(cp)) {
            ConnectPoint dstcp = linkMap.get(cp);
            Port dstport = deviceService.getPort(dstcp.deviceId(), dstcp.port());
            if (dstport.isEnabled()) {
                // add link
                Link link = new DefaultLink(PID, cp, dstcp, Link.Type.INDIRECT);
                log.info("Adding new port link, {} {}", cp, dstcp);
                annotateLink(link);
            }
        }
    }

    private static final class DeviceAnnotationProvider
        extends AbstractProvider implements DeviceProvider {
        private DeviceAnnotationProvider() {
            super(PID);
        }

        @Override
        public void triggerProbe(DeviceId deviceId) {
        }

        @Override
        public void roleChanged(DeviceId deviceId, MastershipRole newRole) {
        }

        @Override
        public boolean isReachable(DeviceId deviceId) {
            return true;
        }
    }

    private class LnkListener implements LinkListener {
        @Override
        public void event(LinkEvent event) {
            if (event.type() == LinkEvent.Type.LINK_ADDED) {
                annotateLink(event.subject());
            }
        }
    }

    private static final class LinkAnnotationProvider
        extends AbstractProvider implements LinkProvider {
        private LinkAnnotationProvider() {
            super(PID);
        }
    }
}


