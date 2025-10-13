from extrac.extraction import extract_spotify

if __name__ == "__main__":
    sp = extract_spotify()
    

    print("\n🎶 Spotify sample:")
    print(sp.head(5))

    