#ifdef PSP_ENABLE_WASM

extern "C" {
    struct CoolStruct {
        int x;
        int y;
    };
    void hello_world();
    *CoolStruct cool_function(int x, int y);
}

#endif