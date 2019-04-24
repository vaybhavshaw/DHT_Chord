service SuperNodeInterface {
            bool ping(),
            string getNode(),
            string join(1: string ip, 2: string port),
            string postJoin(1: string ip, 2: string port),
            void updateDHT(1: i32 id, 2: string ip, 3: string port)
        }
