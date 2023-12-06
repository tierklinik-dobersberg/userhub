import { Injectable, inject } from "@angular/core";
import { Profile, User } from "@tierklinik-dobersberg/apis";
import { BehaviorSubject, Observable, defer, filter, from, map, repeat, share } from "rxjs";
import { environment } from "src/environments/environment";
import { AUTH_SERVICE } from "../api/connect_clients";

export enum UserExtraKey {
  CalendarID = "calendarId",
  Color = "color",
}

export interface UserProfile extends Profile {
  user: User & {
    extra: {
      fields: {
        [UserExtraKey.CalendarID]?: {
          kind: {
            case: 'stringValue';
            value: string,
          }
        },
        [UserExtraKey.Color]?: {
          kind: {
            case: 'stringValue';
            value: string,
          }
        },
      }
    }
  }
}

export function getCalendarId(user: UserProfile | Profile): string | null {
  const prop = user.user?.extra?.fields[UserExtraKey.CalendarID];

  if (!prop || prop.kind.case !== 'stringValue') {
    return null
  }

  return prop.kind.value;
}

export function getUserColor(profile: UserProfile | Profile): string | null {
  const prop = profile?.user?.extra?.fields[UserExtraKey.Color];

  if (!prop || prop.kind.case !== 'stringValue') {
    return null
  }

  return prop.kind.value;
}

@Injectable({providedIn: 'root'})
export class ProfileService {
  authService = inject(AUTH_SERVICE);

  openProfilePage() {
    window.open(environment.accountService, '_blank')
  }

  profile$ = defer(() => from(this.authService.introspect({})))
    .pipe(
      repeat({delay: 20000}),
      map(response => response.profile!),
      share({connector: () => new BehaviorSubject<Profile | undefined>(undefined)}),
      filter(profile => !!profile),
    ) as Observable<UserProfile>;

  snapshot: UserProfile | null = null;

  get id() {
    return this.snapshot?.user?.id
  }

  calendarId(): string | null {
    return getCalendarId(this.snapshot)
  }

  constructor() {
      this.profile$
        .subscribe(profile => this.snapshot = profile);
  }
}
