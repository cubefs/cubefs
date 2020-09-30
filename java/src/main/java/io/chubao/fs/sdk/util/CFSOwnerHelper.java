package io.chubao.fs.sdk.util;

import io.chubao.fs.sdk.exception.CFSException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class CFSOwnerHelper {
    private static final Log log = LogFactory.getLog(CFSOwnerHelper.class);
    private final String passwdPath = "/etc/passwd";
    private final String groupPath = "/etc/group";
    private final String separator = ":";

    private Map<String, Integer> users;
    private Map<Integer, String> uids;
    private Map<String, Integer> groups;
    private Map<Integer, String> gids;
    private Map<String, Integer> userGroups;

    private enum Type {
        user,
        group
    }

    public void init() throws CFSException {
        users = new HashMap<>();
        groups = new HashMap<>();
        uids = new HashMap<>();
        gids = new HashMap<>();
        userGroups = new HashMap<>();
        load(Type.user);
        load(Type.group);
    }

    public int getUid(String user) throws CFSException {
        Integer uid = users.get(user);
        if (uid == null) {
            throw new RuntimeException("Not found the user: " + user + " in " + passwdPath);
        }
        return uid;
    }

    public String getUser(int uid) {
        String user = uids.get(Integer.valueOf(uid));
        if (user == null) {
            log.warn("Not found the uid: " + uid + " in " + passwdPath);
            return String.valueOf(uid);
        }
        return user;
    }

    public int getGid(String group) throws CFSException {
        Integer uid = users.get(group);
        if (uid == null) {
            throw new RuntimeException("Not found the group: " + group + " in " + groupPath);
        }
        return uid;
    }

    public int getGidByUser(String user) throws CFSException {
        Integer uid = userGroups.get(user);
        if (uid == null) {
            throw new RuntimeException("Not found the group: " + user + " in " + passwdPath);
        }
        return uid;
    }

    public String getGroup(int gid) {
        String group = gids.get(Integer.valueOf(gid));
        if (group == null) {
            log.warn("Not found the uid: " + gid + " in " + passwdPath);
            return String.valueOf(gid);
        }
        return group;
    }

    private void add(String line, Type type) throws Exception {
        String[] fileds = line.split(separator);

        if (type == Type.user) {
            if (fileds.length < 4) {
                throw new RuntimeException("[" + line + "] is invalid.");
            }
            users.put(fileds[0], Integer.valueOf(fileds[2]));
            uids.put(Integer.valueOf(fileds[2]), fileds[0]);
            userGroups.put(fileds[0], Integer.valueOf(fileds[3]));
        }

        if (type == Type.group) {
            if (fileds.length < 3) {
                throw new RuntimeException("[" + line + "] is invalid.");
            }
            groups.put(fileds[0], Integer.valueOf(fileds[2]));
            gids.put(Integer.valueOf(fileds[2]), fileds[0]);
        }
    }

    /*
    TODO: Build a schdular to load by period
     */
    private void load(Type type) throws CFSException {
        try {
            String path = null;
            if (type == Type.user) {
                path = passwdPath;
            } else if (type == Type.group) {
                path = groupPath;
            } else {
                throw new RuntimeException("Not support the type:" + type);
            }
            File file = new File(path);
            if (file.exists() == false) {
                throw new RuntimeException("Not found the system passwd profile.");
            }

            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                add(line, type);
            }
        } catch (Exception ex) {
            throw new CFSException(ex);
        }
    }
}
