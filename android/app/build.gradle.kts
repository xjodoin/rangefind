plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.compose")
}

android {
    namespace = "dev.rangefind.wayfind"
    compileSdk = 35

    defaultConfig {
        applicationId = "dev.rangefind.wayfind"
        // Android Automotive's templates need 29, and a 2026 navigation app
        // wanting a modern WebView and MapLibre has no reason to sit below it.
        minSdk = 29
        targetSdk = 35
        versionCode = 1
        versionName = "0.1.0"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            // No public route index is hosted yet; Directions stays disabled
            // until a base URL is configured in-app.
            buildConfigField("String", "ROUTE_BASE_URL", "\"\"")
        }
        debug {
            isMinifyEnabled = false
            // MapLibre ships ~11 MB of native code per ABI. Debug builds only
            // ever run on a dev machine's emulator or an arm64 handset, so
            // shipping all four just wastes install time and device storage.
            ndk { abiFilters += listOf("arm64-v8a", "x86_64") }
            // 10.0.2.2 is the emulator's route to the host loopback, so a local
            // `route-graph/` (e.g. the Luxembourg test index served by the
            // osm-geo demo server on 5184) is reachable without bundling it.
            buildConfigField("String", "ROUTE_BASE_URL", "\"http://10.0.2.2:5184/route-graph/\"")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    // Projected Android Auto and Android Automotive OS ship mutually exclusive
    // car-app artifacts, so they are separate variants rather than one build.
    flavorDimensions += "platform"
    productFlavors {
        create("mobile") { dimension = "platform" }
        create("automotive") {
            dimension = "platform"
            applicationIdSuffix = ".automotive"
        }
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }
}

// Keep the embedded runtime in lockstep with the repo's built browser bundles
// (npm run build:browser) instead of checking copies into the app module.
val syncRangefindBundles by tasks.registering(Copy::class) {
    from(rootProject.file("../dist")) {
        include("runtime.browser.js", "osm.browser.js", "route.browser.js")
    }
    into(layout.projectDirectory.dir("src/main/assets/rangefind"))
}

tasks.named("preBuild") { dependsOn(syncRangefindBundles) }

dependencies {
    implementation("androidx.core:core-ktx:1.15.0")
    implementation("androidx.activity:activity-compose:1.10.1")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.7")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.8.7")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.8.7")

    val composeBom = platform("androidx.compose:compose-bom:2025.04.00")
    implementation(composeBom)
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-graphics")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.material:material-icons-extended")
    debugImplementation("androidx.compose.ui:ui-tooling")

    // Headless rangefind engine host.
    implementation("androidx.webkit:webkit:1.15.0")

    // Native vector map rendering.
    implementation("org.maplibre.gl:android-sdk:11.11.0")

    // Android Auto. `app-projected` is the host binding for phone-projected
    // head units; the templates themselves come from `app`.
    implementation("androidx.car.app:app:1.7.0")
    "mobileImplementation"("androidx.car.app:app-projected:1.7.0")
    // Android Automotive OS runs the same templates natively, which is also
    // what makes the car UI testable on an emulator.
    "automotiveImplementation"("androidx.car.app:app-automotive:1.7.0")

    // Window size classes drive the phone/tablet/foldable layout switch.
    implementation("androidx.compose.material3:material3-window-size-class")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")

    // Navigation decisions that are pure logic — backoff, geometry, phrasing —
    // are worth testing off-device, where a road test cannot reach them.
    testImplementation("junit:junit:4.13.2")
    // android.jar's org.json is a stub that returns null for everything, so a
    // parser tested against it is not tested at all.
    testImplementation("org.json:json:20240303")
}
