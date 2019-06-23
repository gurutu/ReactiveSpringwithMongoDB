package com.example.profileservice;

import java.net.URI;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ProfileServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProfileServiceApplication.class, args);
	}

}

class ProfileCreateEvent extends ApplicationEvent {
	public ProfileCreateEvent(Profile source) {
		super(source);
	}
}

/**
 * WEbSocket
 * @author Pranav
 *
 */
@Configuration
@AllArgsConstructor
class WebSocketConfigration{
	
	@Bean
	WebSocketHandlerAdapter webSocketHandlerAdapter() {
		return new WebSocketHandlerAdapter();
	}
	
	
	@Bean
	HandlerMapping handlerMapping() {
		return  new SimpleUrlHandlerMapping() {
			{
				setOrder(10);
				setUrlMap( Collections.singletonMap("/ws/profiles", handler()));
			}
		
		};
	}
	
	@SneakyThrows
	private  String jsonForm(ProfileCreateEvent pce) {
		return this.objectMapper.writeValueAsString(pce);
	}
	
	private final ObjectMapper objectMapper;
	
	private final ProfileCreatedEventPublisher profileCreatedEventPublisher;
	
	@Bean
	WebSocketHandler handler() {
		 Flux<ProfileCreateEvent>  share=Flux.create(profileCreatedEventPublisher).share();
			return session->{
				Flux<WebSocketMessage> map = share.map(pce->jsonForm(pce))
						.map(session::textMessage);
		               return session.send(map);
			};
					
//		return new WebSocketHandler() {
//			
//			@Override
//			public Mono<Void> handle(WebSocketSession session) {
//				Flux<WebSocketMessage> map = share.map(pce->jsonForm(pce))
//				.map(session::textMessage);
//               return session.send(map);
//			}
//		};
	}
	
	
}




@RestController
class SseController{
	private final ProfileCreatedEventPublisher publisher;
	private final Flux<ProfileCreateEvent> eventFlux;
	private final ObjectMapper objectMapper;
	
	@SneakyThrows
	private String json(ProfileCreateEvent pce) {
		return this.objectMapper.writeValueAsString(pce);
	}
	
	 SseController(ProfileCreatedEventPublisher publisher ,ObjectMapper objectMapper) {
	this.publisher=publisher;
	this.eventFlux= Flux.create(this.publisher).share();
	this.objectMapper=objectMapper;
	}
	
	@GetMapping(produces= MediaType.TEXT_EVENT_STREAM_VALUE,value="/sse/profiles")
	Flux<ProfileCreateEvent> sse(){
		return this.eventFlux;
				//.map(this::json);
	}
	
}

@Component
class ProfileCreatedEventPublisher implements ApplicationListener<ProfileCreateEvent>,
                                   Consumer<FluxSink<ProfileCreateEvent>>{
	private final BlockingQueue<ProfileCreateEvent> event=
			new LinkedBlockingQueue<>();
	
	private final Executor executor;
	ProfileCreatedEventPublisher(Executor executor){
		this.executor=executor;
	}
	
	@Override
	public void accept(FluxSink<ProfileCreateEvent> profileCreateEventFluxSink) {
        this.executor.execute(()-> {
                        while(true) {
                        	try {
								profileCreateEventFluxSink.next(event.take());
							} catch (InterruptedException e) {
								ReflectionUtils.rethrowRuntimeException(e);
							}
                        }
	             	});		
	}
	@Override
	public void onApplicationEvent(ProfileCreateEvent profileCreateEvent) {
		this.event.offer(profileCreateEvent);
		
	}
}


@Service
@AllArgsConstructor
class ProfileService{
	
	private final ProfileRepositery preProfileRepositery;
	private final ApplicationEventPublisher publisher;
	
	Mono<Profile> create(String email){
		return this.preProfileRepositery.save(new Profile(null,email))
				.doOnSuccess(profile->this.publisher
						.publishEvent(new ProfileCreateEvent(profile)));
	}
	
	
	//Flux Return 0 to N record 
	Flux<Profile> all(){
		return this.preProfileRepositery.findAll();
	}
	
	//Mono will produce 0 or one recode 
	Mono<Profile> byId(String id){
		return this.preProfileRepositery.findById(id);
	}
	
}




//GET /PRofiles
//GET /PROfile/{id}
//Post /profile
@RestController
@RequestMapping("/profiles")
@AllArgsConstructor
class ProfileRestController{
	
	private final ProfileService preProfileService;
	
	@GetMapping("/{id}")
	Publisher<Profile> byId(@PathVariable String id){
		return this.preProfileService.byId(id);
	}
	
	@GetMapping
	Publisher<Profile> all(){
		return this.preProfileService.all();
	}
	
	@PostMapping
	Publisher<ResponseEntity<Profile>> create(@RequestBody Profile profile){
		return this.preProfileService.create(profile.getEmail())
				.map(p-> ResponseEntity
						.created(URI.create("/profiles/"+p.getId()))
						.build());
	}
	
	
	
}


@Log4j2
@Component
@AllArgsConstructor
class Initializer implements ApplicationListener<ApplicationReadyEvent>{

	private final ProfileRepositery profileRepositery;
	
	
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		Flux<Profile> profiles = Flux.just("A","B","C","D")
		.map(email->new Profile(null,email))
		.flatMap(this.profileRepositery::save);
		
		this.profileRepositery.deleteAll()
		.thenMany(profiles)
		.thenMany(this.profileRepositery.findAll())
		.subscribe(log::info);
		//new Consumer<Profile>() {
//			public void accept(Profile t) {
//				System.out.println("Profile : "+t.getEmail());
//			};
//		}
	}
	
}

interface ProfileRepositery extends ReactiveMongoRepository<Profile,String>{
	
}


@Data
@AllArgsConstructor
@NoArgsConstructor
@Document
class Profile{
	
	@Id
	private String id;
	
	private String email;
	
}
