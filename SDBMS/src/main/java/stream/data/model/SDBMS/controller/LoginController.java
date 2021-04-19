package stream.data.model.SDBMS.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import stream.data.model.SDBMS.model.*;
import stream.data.model.SDBMS.services.MyUserDetailsService;
import stream.data.model.SDBMS.util.DbUtil;
import stream.data.model.SDBMS.util.JwtUtil;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Collectors;
import org.json.*;

@Controller
public class LoginController {
    @Autowired
    private AuthenticationManager authManager;

    @Autowired
    private MyUserDetailsService userDetailsService;

    @Autowired
    private JwtUtil jwtTokenUtil;

    @Autowired
    private DbUtil dbUtil;

    @GetMapping("/hello")
    public String hello(Model theModel) {
        System.out.println("hello");
        theModel.addAttribute("theData", new java.util.Date());
        return "helloWorld";
    }

    @GetMapping("/register")
    public String registerPage() {
        return "register";
    }

    @PostMapping("/register")
    public String registerNew(HttpServletRequest req, Model theModel) {
        StreamUser curr = new StreamUser(req.getParameter("username")
                , req.getParameter("firstname")
                , req.getParameter("lastname")
                , req.getParameter("password")
                , req.getParameter("email")
                , req.getParameter("organization") );

        if(dbUtil.registerNewUser(curr) == 1) {
            return "login";
        } else {
            theModel.addAttribute("isError", "Error creating new user");
            return "register";
        }


    }

    @GetMapping("/userProfile")
    public String userProfile(HttpServletRequest req, Model theModel) throws Exception {
        System.out.println("get /userProfile");
        Cookie[] cookies = req.getCookies();
        if(cookies != null) {
            for(Cookie c : cookies) {
                if(c.getName().equals("sdbmsAppUser")) {
                    StreamUser streamUser = dbUtil.getUserDetails(c.getValue());
                    ArrayList<Stream> userStreamDetails = dbUtil.getStreamDetails(c.getValue());
                    theModel.addAttribute("streamUser", streamUser);
                    theModel.addAttribute("userStreamDetails", userStreamDetails);
                }
            }
        }

        return "dashboard/userProfile";
    }

    @GetMapping("/streamDetails")
    public String streamDetailsCall(HttpServletRequest req, Model theModel) throws Exception {

        Cookie[] cookies = req.getCookies();
        if(cookies != null) {
            for(Cookie c : cookies) {
                if(c.getName().equals("sdbmsAppUser")) {
                    StreamUser streamUser = dbUtil.getUserDetails(c.getValue());
                    ArrayList<Stream> userStreamDetails = dbUtil.getStreamDetails(c.getValue());
                    theModel.addAttribute("streamUser", streamUser);
                    theModel.addAttribute("userStreamDetails", userStreamDetails);
                    Stream particularStream;
                    for(Stream s : userStreamDetails) {
                        if(s.getUsername().equals(req.getParameter("username")) && s.getStreamid().equals(req.getParameter("streamid")) ) {
                            particularStream = new Stream(s.getUsername(),
                                    s.getStreamid(), s.getSname(), s.getSource(),
                                    s.getLink(), s.getWindowType(), s.getWindowVelocity(), s.getWindowSize());
                            theModel.addAttribute("particularStream", particularStream);
                            break;
                        }
                    }

                    ArrayList<QueryStream> queries = dbUtil.getQueries(req.getParameter("username"),req.getParameter("streamid"));

                    theModel.addAttribute("streamQueries", queries);

                }
            }
        }

        return "dashboard/streamDetails";
    }


    @GetMapping("/createNewStream")
    public String createNewStreamForm() {
        return "createNewStreamForm";
    }

    @PostMapping("/newStreamDetails")
    public String newStreamDetails(HttpServletRequest req) throws Exception {
        JSONObject reqBody = new JSONObject(req.getReader().lines().collect(Collectors.joining(System.lineSeparator())));
        JSONObject parameters = new JSONObject(reqBody.getString("json_data"));
        System.out.println(reqBody.getString("json_data"));

        JSONObject cmdParas = new JSONObject();

        // userid
        Cookie[] cookies = req.getCookies();
        if(cookies != null) {
            for(Cookie c : cookies) {
                if(c.getName().equals("sdbmsAppUser")) {
                    cmdParas.put("userid", c.getValue());
                }
            }
        }

        // streamid;
        Random r = new Random( System.currentTimeMillis() );
        int streamid = (1 + r.nextInt(2)) * 10000 + r.nextInt(10000);

        cmdParas.put("streamid", String.valueOf(streamid));

        //streamName
        cmdParas.put("streamName", parameters.getString("stream_name"));

        // dataSrc
        cmdParas.put("dataSrc", parameters.getString("data_src"));

        // dataFileSrc
        cmdParas.put("dataSrc", parameters.getString("data_file_src"));

        // queries
        JSONArray queries = new JSONArray();
        JSONArray que = parameters.getJSONArray("queries");

        for(int i = 0 ; i < que.length() ; i++) {
            queries.put(que.getString(i));
        }

        cmdParas.put("queries", queries);


        // dataAttributes
        JSONArray dataAttributes = new JSONArray();
        JSONArray dataAttr = parameters.getJSONArray("elementsArray");

        for(int i = 0 ; i < que.length() ; i++) {
            queries.put(que.getString(i));
        }

        return "redirect:userProfile";
    }

//    @GetMapping("/userProfile")
//    public String userProfile(Model theModel) {


//        ProcessBuilder processBuilder = new ProcessBuilder();
//        final ClassLoader classLoader = getClass().getClassLoader();
//        final File scriptFile = new File(classLoader.getResource("inputMonitorCreator.sh").getFile());
//        processBuilder.command(scriptFile.getPath(), "Alay_456");
//
//        try {
//
//            Process process = processBuilder.start();
//
//            StringBuilder output = new StringBuilder();
//
//            BufferedReader reader = new BufferedReader(
//                    new InputStreamReader(process.getInputStream()));
//
//            String line;
//            while ((line = reader.readLine()) != null) {
//                output.append(line + "\n");
//            }
//
//            int exitVal = process.waitFor();
//            if (exitVal == 0) {
//                System.out.println("Success! creating inputMonitorCreator");
//                System.out.println(output);
//            } else {
//                //abnormal...
//                System.out.println("abnormal :: inputMonitorCreator");
//            }
//
//        } catch (Exception e) {
//            System.out.println("inputMonitorCreator process exception" + e);
//        }
//
//
//        return "pages/userProfile";

//    }

    @GetMapping("/login")
    public String login(Model theModel) {
        return "login";
    }

    @RequestMapping(value = "/authenticate", method = RequestMethod.POST)
    public ResponseEntity<?> createAuthenticationToken(@RequestBody AuthenticationRequest authReq) throws Exception {
        try {
            authManager.authenticate(
                    new UsernamePasswordAuthenticationToken(authReq.getUsername(), authReq.getPassword())
            );

        } catch (BadCredentialsException be) {
            throw new Exception("Incorrect username / password : ", be);
        }

        final UserDetails userDetails = userDetailsService.loadUserByUsername(authReq.getUsername());
        final String jwt = jwtTokenUtil.generateToken(userDetails);
        final String userName = jwtTokenUtil.extractUsername(jwt);
        return  ResponseEntity.ok(new AuthenticationResponse(jwt));

    }

}