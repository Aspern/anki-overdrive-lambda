package de.msg.iot.anki.rest;

import de.msg.iot.anki.controller.kafka.KafkaVehicleController;
import de.msg.iot.anki.entity.Setup;
import de.msg.iot.anki.entity.Vehicle;
import de.msg.iot.anki.scenario.AntiCollision;
import de.msg.iot.anki.scenario.Collision;
import de.msg.iot.anki.scenario.Scenario;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Path("/setup")
public class SetupRestHandler {

    private static Map<String, KafkaVehicleController> controller = new HashMap<>();
    private static Map<String, Future<Void>> runningScenarios = new HashMap<>();
    private static List<String> scenarios = new ArrayList<String>() {{
        add("collision");
        add("anti-collision");
    }};
    private static ExecutorService threadpool = Executors.newCachedThreadPool();

    private final EntityManagerFactory factory = Persistence.createEntityManagerFactory("anki");
    private final EntityManager manager = factory.createEntityManager();


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response list() {
        List<Setup> list = manager.createQuery("select d from Setup d")
                .getResultList();

        return Response.ok(list).build();
    }

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@PathParam("id") long id) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null)
            return Response.status(404).build();

        return Response.ok(setup).build();
    }

    @GET
    @Path("/{id}/vehicle")
    @Produces(MediaType.APPLICATION_JSON)
    public Response vehicle(@PathParam("id") long id) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null)
            return Response.status(404).build();

        return Response.ok(setup.getVehicles()).build();
    }

    @GET
    @Path("/{id}/vehicle/{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response vehicle(@PathParam("id") long id, @PathParam("uuid") String uuid) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null)
            return Response.status(404).build();

        for (Vehicle vehicle : setup.getVehicles()) {
            if (uuid.equals(vehicle.getUuid()))
                return Response.ok(vehicle).build();
        }

        return Response.status(404).build();
    }

    @POST
    @Path("/{id}/vehicle/{uuid}/set-speed")
    @Produces(MediaType.APPLICATION_JSON)
    public Response setSpeed(@PathParam("id") long id, @PathParam("uuid") String uuid, @QueryParam("speed") int speed, @QueryParam("acceleration") int acceleration) {
        Response response = vehicle(id, uuid);

        if (response.getStatus() != 200)
            return Response.status(response.getStatus()).build();

        try {
            Vehicle vehicle = response.readEntity(Vehicle.class);
            controller(vehicle.getUuid()).setSpeed(speed, acceleration);
            return Response.ok().build();
        } catch (Exception e) {
            return Response.status(500).entity(e).build();
        }
    }

    @GET
    @Path("/{id}/track")
    @Produces(MediaType.APPLICATION_JSON)
    public Response track(@PathParam("id") long id) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null || setup.getTrack() == null)
            return Response.status(404).build();

        return Response.ok(setup.getTrack()).build();
    }


    @GET
    @Path("/{id}/scenario")
    @Produces(MediaType.APPLICATION_JSON)
    public Response scenario(@PathParam("id") long id) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null)
            return Response.status(404).build();

        return Response.ok(scenarios).build();
    }

    @POST
    @Path("/{id}/scenario/{name}/start")
    @Produces(MediaType.APPLICATION_JSON)
    public Response startScenario(@PathParam("id") long id, @PathParam("name") String name) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null || !scenarios.contains(name))
            return Response.status(404).build();

        switch (name) {
            case "collision":
                startScenario(new Collision(setup));
                return Response.ok().build();
            case "anti-collision":
                startScenario(new AntiCollision(setup));
                return Response.ok().build();
            default:
                return Response.status(404).build();
        }
    }

    @POST
    @Path("/{id}/scenario/{name}/stop")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stopScenario(@PathParam("id") long id, @PathParam("name") String name) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null || !scenarios.contains(name))
            return Response.status(404).build();

        if (stopScenario(name))
            return Response.ok().build();

        return Response.status(404).build();
    }

    private KafkaVehicleController controller(String uuid) {
        if (!controller.containsKey(uuid))
            controller.put(uuid, new KafkaVehicleController(uuid));

        return controller.get(uuid);
    }

    private boolean startScenario(Scenario scenario) {
        if (!runningScenarios.containsKey(scenario.getName())) {
            runningScenarios.put(scenario.getName(), threadpool.submit(scenario));
            return true;
        }
        return false;
    }

    private boolean stopScenario(String name) {
        if (runningScenarios.containsKey(name)) {
            runningScenarios.get(name).cancel(true);
            return true;
        }
        return false;
    }

}
