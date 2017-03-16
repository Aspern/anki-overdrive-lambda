package de.msg.iot.anki.rest;

import de.msg.iot.anki.controller.kafka.KafkaVehicleController;
import de.msg.iot.anki.entity.Setup;
import de.msg.iot.anki.entity.Vehicle;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/setup")
public class SetupRestHandler {

    private static Map<String, KafkaVehicleController> controller = new HashMap<>();

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
    public Response getVehicles(@PathParam("id") long id) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null)
            return Response.status(404).build();

        return Response.ok(setup.getVehicles()).build();
    }

    @GET
    @Path("/{id}/vehicle/{uuid}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVehicles(@PathParam("id") long id, @PathParam("uuid") String uuid) {
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
        Response response = getVehicles(id, uuid);

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
    public Response getTrack(@PathParam("id") long id) {
        Setup setup = manager.find(Setup.class, id);

        if (setup == null || setup.getTrack() == null)
            return Response.status(404).build();

        return Response.ok(setup.getTrack()).build();
    }

    private KafkaVehicleController controller(String uuid) {
        if (!controller.containsKey(uuid))
            controller.put(uuid, new KafkaVehicleController(uuid));

        return controller.get(uuid);
    }

}
