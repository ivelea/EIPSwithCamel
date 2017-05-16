package module2.multicast;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrderRouterWithParallelMulticast {

    //~ ----------------------------------------------------------------------------------------------------------------
    //~ Static fields/initializers 
    //~ ----------------------------------------------------------------------------------------------------------------

    private static Logger logger = LoggerFactory.getLogger(OrderRouterWithParallelMulticast.class);

    //~ ----------------------------------------------------------------------------------------------------------------
    //~ Methods 
    //~ ----------------------------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // create CamelContext
        CamelContext context = new DefaultCamelContext();

        // connect to embedded ActiveMQ JMS broker
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        context.addComponent("jms", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));

        // add our route to the CamelContext
        context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    // load file orders from src/data into the JMS queue
                    from("file:src/data?noop=true").routeId("File system polling route")
                    	.to("jms:incomingOrders");

                    // content-based router

                    //J-
                    from("jms:incomingOrders").routeId("JMS incoming orders route")
                    .wireTap("jms:orderAudit")
                    .choice()
                        .when(header("CamelFileName").endsWith(".xml")).to("jms:xmlOrders")
                        .when(header("CamelFileName").regex("^.*(csv|csl)$")).to("jms:csvOrders")
                        .otherwise().to("jms:badOrders");
                    //J+

                    ExecutorService executor = Executors.newFixedThreadPool(12);
                    
                    //J-
                    from("jms:xmlOrders").routeId("XML orders processing route")
                    .filter(xpath("/order[not(@test)]"))
                    .multicast()
                        .parallelProcessing()
                        .executorService(executor)
                        .to("jms:marginCompQueue", "jms:riskMgmtQueue");
                    //J+

                    // test that our route is working
                    from("jms:marginCompQueue").routeId("Margin calculation route")
                    	.process(new Processor() {
	                        public void process(Exchange exchange) throws Exception {
	                            logger.info(" => Margin limit calculation component received order: " +
	                                exchange.getIn().getHeader("CamelFileName"));
	                        }
                        });
                    from("jms:riskMgmtQueue").routeId("Risk management route")
                    	.process(new Processor() {
                            public void process(Exchange exchange) throws Exception {
                                logger.info(" => Risk management component received order: " +
                                    exchange.getIn().getHeader("CamelFileName"));
                            }
                        });
                    
                    from("jms:orderAudit").routeId("Audit department route")
                    	.process(new Processor() {
	                        @Override
	                        public void process(Exchange exchange) throws Exception {
	                            logger.info(" => The Audit department received the order:" + exchange.getIn().getHeader(Exchange.FILE_NAME));
	                        }
                    });
                }
            });

        // start the route and let it do its work
        context.start();
        Thread.sleep(2000);

        // stop the CamelContext
        context.stop();

        System.exit(0);
    }
}
