package org.inesctec.flexcomm.provider.netcfglinks;

import java.util.Set;

import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Link;
import org.onosproject.net.LinkKey;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.net.config.NetworkConfigListener;
import org.onosproject.net.config.NetworkConfigRegistry;
import org.onosproject.net.config.basics.BasicLinkConfig;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.link.DefaultLinkDescription;
import org.onosproject.net.link.LinkProvider;
import org.onosproject.net.link.LinkProviderRegistry;
import org.onosproject.net.link.LinkProviderService;
import org.onosproject.net.provider.AbstractProvider;
import org.onosproject.net.provider.ProviderId;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

@Component(immediate = true)
public class NetworkConfigLinksProvider extends AbstractProvider implements LinkProvider {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private static final String PROVIDER_NAME = "org.inesctec.flexcomm.provider.netcfglinks";

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected LinkProviderRegistry providerRegistry;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected DeviceService deviceService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected NetworkConfigRegistry netCfgService;

  private LinkProviderService providerService;

  private final InternalDeviceListener deviceListener = new InternalDeviceListener();
  private final InternalConfigListener cfgListener = new InternalConfigListener();

  private Set<LinkKey> configuredLinks = Sets.newConcurrentHashSet();

  public NetworkConfigLinksProvider() {
    super(new ProviderId("netcfglinks", PROVIDER_NAME));
  }

  @Activate
  protected void activate() {
    providerService = providerRegistry.register(this);
    deviceService.addListener(deviceListener);
    netCfgService.addListener(cfgListener);
    createLinks();
    loadDevices();
    log.info("Activated");
  }

  @Deactivate
  protected void deactivate() {
    providerRegistry.unregister(this);
    deviceService.removeListener(deviceListener);
    netCfgService.removeListener(cfgListener);
    providerService = null;
    log.info("Deactivated");
  }

  private void createLinks() {
    netCfgService.getSubjects(LinkKey.class).forEach(linkKey -> configuredLinks.add(linkKey));
  }

  private void loadDevices() {
    deviceService.getAvailableDevices()
        .forEach(d -> deviceService.getPorts(d.id()).forEach(p -> addLinks(d.id(), p.number())));
  }

  private boolean matchLink(LinkKey linkKey, DeviceId deviceId, PortNumber portNumber) {
    return (linkKey.src().deviceId().equals(deviceId) && linkKey.src().port().equals(portNumber))
        || (linkKey.dst().deviceId().equals(deviceId) && linkKey.dst().port().equals(portNumber));
  }

  private void addLinks(DeviceId deviceId, PortNumber portNumber) {
    configuredLinks.stream().filter(lk -> matchLink(lk, deviceId, portNumber))
        .forEach(lk -> providerService.linkDetected(
            new DefaultLinkDescription(lk.src(), lk.dst(), Link.Type.DIRECT, true, DefaultAnnotations.EMPTY)));
  }

  private class InternalDeviceListener implements DeviceListener {

    @Override
    public void event(DeviceEvent event) {
      Device device = event.subject();
      Port port = event.port();
      if (device == null) {
        log.error("Device is null.");
        return;
      }
      log.trace("{} {} {}", event.type(), event.subject(), event);
      final DeviceId deviceId = device.id();
      switch (event.type()) {
        case DEVICE_ADDED:
        case DEVICE_UPDATED:
          deviceService.getPorts(deviceId).forEach(p -> addLinks(deviceId, p.number()));
          break;
        case DEVICE_REMOVED:
        case DEVICE_SUSPENDED:
          log.debug("Device removed {}", deviceId);
          providerService.linksVanished(deviceId);
          break;
        case DEVICE_AVAILABILITY_CHANGED:
          if (deviceService.isAvailable(deviceId)) {
            log.debug("Device up {}", deviceId);
            deviceService.getPorts(deviceId).forEach(p -> addLinks(deviceId, p.number()));
          } else {
            log.debug("Device down {}", deviceId);
            providerService.linksVanished(deviceId);
          }
          break;
        case PORT_ADDED:
        case PORT_UPDATED:
          if (port.isEnabled()) {
            addLinks(deviceId, port.number());
          } else {
            log.debug("Port down {}", port);
            providerService.linksVanished(new ConnectPoint(port.element().id(), port.number()));
          }
          break;
        case PORT_REMOVED:
          log.debug("Port removed {}", port);
          providerService.linksVanished(new ConnectPoint(port.element().id(), port.number()));
          break;
        case PORT_STATS_UPDATED:
          break;
        default:
          log.debug("Unknown event {}", event);
      }
    }
  }

  private class InternalConfigListener implements NetworkConfigListener {

    private void addLink(LinkKey linkKey) {
      DefaultLinkDescription linkDescription = new DefaultLinkDescription(linkKey.src(), linkKey.dst(),
          Link.Type.DIRECT, true, DefaultAnnotations.EMPTY);
      configuredLinks.add(linkKey);
      providerService.linkDetected(linkDescription);
    }

    private void removeLink(LinkKey linkKey) {
      DefaultLinkDescription linkDescription = new DefaultLinkDescription(linkKey.src(), linkKey.dst(),
          Link.Type.DIRECT);
      configuredLinks.remove(linkKey);
      providerService.linkVanished(linkDescription);
    }

    @Override
    public void event(NetworkConfigEvent event) {
      if (event.configClass().equals(BasicLinkConfig.class)) {
        log.info("net config event of type {} for basic link {}", event.type(), event.subject());
        LinkKey linkKey = (LinkKey) event.subject();
        if (event.type() == NetworkConfigEvent.Type.CONFIG_ADDED) {
          addLink(linkKey);
          event.config().ifPresent(config -> {
            if (((BasicLinkConfig) config).isBidirectional()) {
              addLink(LinkKey.linkKey(linkKey.dst(), linkKey.src()));
            }
          });
        } else if (event.type() == NetworkConfigEvent.Type.CONFIG_REMOVED) {
          removeLink(linkKey);
          event.prevConfig().ifPresent(config -> {
            if (((BasicLinkConfig) config).isBidirectional()) {
              removeLink(LinkKey.linkKey(linkKey.dst(), linkKey.src()));
            }
          });
        }
        log.info("Link reconfigured");
      }
    }
  }
}
