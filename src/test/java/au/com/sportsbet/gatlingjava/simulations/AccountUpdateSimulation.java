package au.com.sportsbet.gatlingjava.simulations;

import au.com.sportsbet.gatlingjava.kafka.KafkaMessageProvider;
import au.com.sportsbet.gatlingjava.kafka.MessageProvider;
import au.com.sportsbet.gatlingjava.kafka.MessageService;
import com.flutter.uki.cbse.AccountUpdate;
import com.flutter.uki.cbse.Address;
import com.flutter.uki.cbse.BrandName;
import com.flutter.uki.cbse.CustomerInfo;
import com.flutter.uki.cbse.EventSourceType;
import com.flutter.uki.cbse.GamstopStatus;
import com.flutter.uki.cbse.RegulatorExclusion;
import com.flutter.uki.cbse.SelfExclusion;
import com.flutter.uki.cbse.SelfExclusionType;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.rampUsers;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static java.lang.String.format;
import static java.util.Collections.singletonList;


public class AccountUpdateSimulation extends Simulation {
    static AtomicBoolean messageSent = new AtomicBoolean(false);
    AccountUpdate accountUpdate;

    {
        accountUpdate = createAccountUpdate(UUID.randomUUID().toString(),
                RandomStringUtils.randomAlphanumeric(10),
                "ddd",
                "ddd", EventSourceType.ACCOUNT_UPDATE);

    }

    ScenarioBuilder scn = scenario("Update account")
            .exec(session -> {
                publishMessage("customers", accountUpdate);
                return session;
            })
            .exec(
                    session -> {
                        publishMessage("customers", accountUpdate);
                        return session;
                    }
            );



    {
        setUp(scn.injectOpen(rampUsers(100).
                during(java.time.Duration.ofMinutes(1)))).
                assertions(global().successfulRequests().
                        count().around(1L, 1000L));
    }

    public static AccountUpdate createAccountUpdate(String requestId, String customerId, String firstName, String lastName, EventSourceType eventSourceType) {
        Address address = new Address("GB", "Calle de Atocha,", "27", "Just ring the Bell", "Cluj-Napoca", "28001");

        CustomerInfo customerInfo = new CustomerInfo(customerId, firstName + ".un", firstName, lastName, 123456, address, format("%s@%s.com", firstName, lastName), "07345678901", "000456123789");

        RegulatorExclusion regulatorExclusion = RegulatorExclusion.newBuilder().setSourceBrand(BrandName.BF).setStart(1L).setStatus(GamstopStatus.N).build();

        SelfExclusion selfExclusion = SelfExclusion.newBuilder().setStart(234L).setEnd(456L).setSourceBrand(BrandName.PS).setType(SelfExclusionType.VOLUNTARY).build();

        return AccountUpdate.newBuilder().setId(requestId).setEventSource(eventSourceType).setCustomerInfo(customerInfo).setRegulatorExclusion(singletonList(regulatorExclusion)).setSelfExclusion(singletonList(selfExclusion)).build();
    }

    private static <T> void publishMessage(String topic, T message) {
        MessageProvider messageProvider = new KafkaMessageProvider("localhost:9092",
                "http://localhost:8081");
        MessageService messageService = new MessageService(messageProvider);
        messageService.publishMessage(topic, message);
        messageSent.set(true);
    }

    @Override
    public void after() {
        assert messageSent.get() : "No message was sent";

    }
}
